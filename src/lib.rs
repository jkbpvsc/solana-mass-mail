use std::{
    collections::{HashMap, HashSet},
    thread::sleep,
    time::{Duration, Instant},
};

use log::{debug, error, info};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    hash::Hash, instruction::Instruction, message::VersionedMessage, pubkey::Pubkey,
    signers::Signers, transaction::VersionedTransaction,
};

#[derive(Clone)]
pub struct MailBuilder<T: Signers + Sized + Clone> {
    message: VersionedMessage,
    keypairs: T,
}

impl<T: Signers + Sized + Clone> MailBuilder<T> {
    pub fn new(message: VersionedMessage, signers: T) -> Self {
        Self {
            message,
            keypairs: signers,
        }
    }

    pub fn new_legacy_transaction(
        instructions: &[Instruction],
        payer: Option<&Pubkey>,
        signers: T,
    ) -> Self {
        let message =
            VersionedMessage::Legacy(solana_sdk::message::Message::new(instructions, payer));

        Self {
            message,
            keypairs: signers,
        }
    }

    pub(crate) fn make_signed_versioned_transaction(
        &self,
        blockhash: Hash,
    ) -> Result<VersionedTransaction, solana_sdk::signer::SignerError> {
        let mut message = self.message.clone();
        message.set_recent_blockhash(blockhash);

        VersionedTransaction::try_new(message, &self.keypairs)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum MassmailError {
    #[error("Client error: {0}")]
    ClientError(#[from] solana_client::client_error::ClientError),
    #[error("Transaction error")]
    TransactionTimeout,
    #[error("Signer error: {0}")]
    SignerError(#[from] solana_sdk::signer::SignerError),
    #[error("Too many failures")]
    TooManyFailures,
}

pub struct MassmailCfg {
    pub max_at_once: usize,
    pub max_failures: usize,
    pub tx_timeout: std::time::Duration,
    pub commitment: solana_sdk::commitment_config::CommitmentConfig,
    pub dry_run: bool,
}

impl MassmailCfg {
    pub const DEFAULT: Self = Self {
        max_at_once: 30,
        max_failures: 5,
        tx_timeout: Duration::from_secs(45),
        commitment: solana_sdk::commitment_config::CommitmentConfig::confirmed(),
        dry_run: false,
    };
}

fn dryrun<T: Signers + Clone>(
    rpc_client: &RpcClient,
    tx_builders: Vec<MailBuilder<T>>,
) -> Result<(), MassmailError> {
    let start = Instant::now();
    let total_to_send = tx_builders.len();

    let blockhash = rpc_client.get_latest_blockhash()?;

    for tx_builder in tx_builders {
        let tx = tx_builder.make_signed_versioned_transaction(blockhash)?;

        match rpc_client.simulate_transaction(&tx) {
            Ok(res) => {
                debug!("Simulated transaction: {:?}", res.value);

                if let Some(e) = res.value.err {
                    error!("Transaction failed: {} {:#?}", e, res.value.logs);
                }
            }
            Err(e) => {
                error!("Failed to simulate transaction: {:?}", e);
            }
        }
    }

    info!(
        "All ({}) transactions dry run, took: {:?}",
        total_to_send,
        start.elapsed()
    );

    Ok(())
}

pub fn massmail<T: Signers + Clone>(
    rpc_client: &RpcClient,
    mut outstanding_to_send: Vec<MailBuilder<T>>,
    cfg: MassmailCfg,
) -> Result<(), MassmailError> {
    if cfg.dry_run {
        return dryrun(rpc_client, outstanding_to_send);
    }

    let start = Instant::now();
    let mut to_send_next = vec![];
    let mut to_confirm = HashMap::new();
    let mut landed = HashSet::new();

    let mut blockhash;
    let total_to_send = outstanding_to_send.len();

    let mut failures = 0;

    while landed.len() != total_to_send {
        // Fill up the to_send_next
        while to_send_next.len() < (cfg.max_at_once - to_confirm.len())
            && !outstanding_to_send.is_empty()
            && to_confirm.len() < cfg.max_at_once
        {
            to_send_next.push(outstanding_to_send.pop().unwrap());
        }

        debug!("Sending {} transactions", to_send_next.len());

        blockhash = rpc_client.get_latest_blockhash().unwrap();

        while let Some(tx_builder) = to_send_next.pop() {
            let tx = tx_builder.make_signed_versioned_transaction(blockhash)?;
            let sig = rpc_client.send_transaction(&tx);

            match sig {
                Ok(sig) => {
                    debug!("Sent transaction: {:?}", sig);

                    to_confirm.insert(sig, (tx_builder, blockhash, Instant::now()));
                }
                Err(e) => {
                    failures += 1;

                    if failures > cfg.max_failures {
                        error!("Too many failures, exiting");
                        return Err(MassmailError::TooManyFailures);
                    }

                    outstanding_to_send.push(tx_builder.clone());
                    debug!("Failed to send transaction: {:?}", e);
                }
            }
        }

        // Confirm transactions
        let mut to_remove_from_to_confirm = vec![];
        for (sig, (tx_builder, blockhash, send_time)) in to_confirm.iter() {
            debug!("Checking transaction: {:?}", sig);
            let status = rpc_client.get_signature_status_with_commitment(sig, cfg.commitment);

            match status {
                Ok(None) => {
                    // Transaction not confirmed yet
                    let blockhash_not_found =
                        rpc_client.is_blockhash_valid(blockhash, cfg.commitment)?;

                    if !blockhash_not_found && send_time.elapsed() >= cfg.tx_timeout {
                        debug!("Blockhash expired: {:?}", sig);

                        to_remove_from_to_confirm.push(*sig);
                        outstanding_to_send.push(tx_builder.clone());
                    }
                }
                Ok(Some(status)) => {
                    debug!("Transaction landed: {:?}, success: {}", sig, status.is_ok());
                    landed.insert(*sig);
                    to_remove_from_to_confirm.push(*sig);

                    if let Err(e) = status {
                        error!("Transaction failed: {:?}", e);
                    }
                }
                Err(e) => {
                    debug!("Failed to get signature status: {:?}", e);
                }
            }
        }

        for sig in to_remove_from_to_confirm {
            to_confirm.remove(&sig);
        }

        sleep(Duration::from_millis(500));

        debug!(
            "Landed: {}/{}, to_confirm: {}",
            landed.len(),
            total_to_send,
            to_confirm.len(),
        );
    }

    info!(
        "All ({}) transactions landed, took: {:?}",
        total_to_send,
        start.elapsed()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use solana_sdk::{signature::Keypair, signer::Signer};

    use super::*;
    #[test]
    fn test_send_transactions() {
        // Create a mock RPC client
        let rpc_client = RpcClient::new_mock("");

        // Create some mock transactions
        let mut signers: Vec<Arc<dyn Signer>> = vec![];

        signers.push(Arc::new(Keypair::new()));

        // Create a TxBuilder
        let tx_builder = MailBuilder::new(VersionedMessage::default(), signers);

        // Create a vector of TxBuilders
        let mut tx_builders = vec![];

        tx_builders.push(tx_builder);

        // Set the maximum failures and timeout values
        let cfg = MassmailCfg::DEFAULT;

        // Call the function under test
        let res = massmail(&rpc_client, tx_builders, cfg);
        assert!(res.is_err());
    }
}
