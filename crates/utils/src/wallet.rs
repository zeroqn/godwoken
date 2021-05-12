use anyhow::{anyhow, Context, Result};
use ckb_crypto::secp::{Privkey, SECP256K1};
use faster_hex::hex_decode;
use gw_common::blake2b::new_blake2b;
use gw_config::WalletConfig;
use gw_types::{
    bytes::Bytes,
    packed::{Script, Transaction},
    prelude::{Entity, Unpack},
};
use sha3::{Digest, Keccak256};

use crate::transaction_skeleton::TransactionSkeleton;

pub struct Wallet {
    privkey: Privkey,
    lock: Script,
    eth_address: [u8; 20],
}

impl Wallet {
    pub fn from_config(config: &WalletConfig) -> Result<Self> {
        let lock = config.lock.clone().into();
        let privkey_raw = {
            let content = std::fs::read_to_string(&config.privkey_path)
                .with_context(|| "read wallet privkey")?;
            let content = content.trim_start_matches("0x").trim();
            assert_eq!(content.as_bytes().len(), 64, "invalid privkey length");
            let mut decoded = [0u8; 32];
            hex_decode(content.as_bytes(), &mut decoded)?;
            decoded
        };
        let privkey = Privkey::from_slice(&privkey_raw);
        let eth_address = {
            let privkey = secp256k1::SecretKey::from_slice(&privkey_raw)?;
            let pubkey = secp256k1::PublicKey::from_secret_key(&SECP256K1, &privkey);

            let mut hasher = Keccak256::new();
            hasher.update(&pubkey.serialize_uncompressed()[1..]);
            let buf = hasher.finalize();
            let mut pubkey_hash = [0u8; 20];
            pubkey_hash.copy_from_slice(&buf[12..]);
            pubkey_hash
        };
        let wallet = Wallet {
            privkey,
            lock,
            eth_address,
        };
        Ok(wallet)
    }

    pub fn eth_address(&self) -> [u8; 20] {
        self.eth_address
    }

    pub fn lock_script(&self) -> &Script {
        &self.lock
    }

    // sign message
    pub fn sign_message(&self, msg: [u8; 32]) -> Result<[u8; 65]> {
        let signature = self
            .privkey
            .sign_recoverable(&msg.into())
            .map_err(|err| anyhow!("signing error: {}", err))?;
        let mut inner = [0u8; 65];
        inner.copy_from_slice(&signature.serialize());
        Ok(inner)
    }

    pub fn sign_tx_skeleton(&self, tx_skeleton: TransactionSkeleton) -> Result<Transaction> {
        let signature_entries = tx_skeleton.signature_entries();
        let dummy_signatures = {
            let mut sigs = Vec::new();
            sigs.resize(signature_entries.len(), [0u8; 65]);
            sigs
        };
        // seal a dummy tx for calculation
        let tx = tx_skeleton
            .seal(&signature_entries, dummy_signatures)?
            .transaction;
        let tx_hash = {
            let mut hasher = new_blake2b();
            hasher.update(tx.raw().as_slice());
            let mut hash = [0u8; 32];
            hasher.finalize(&mut hash);
            hash
        };
        let mut signatures = Vec::with_capacity(signature_entries.len());
        for entry in &signature_entries {
            let mut hasher = new_blake2b();
            // hash tx_hash
            hasher.update(&tx_hash);
            // hash the first witness: len | witness
            let first_witness: Bytes = tx
                .witnesses()
                .get(entry.indexes[0])
                .expect("get first witness")
                .unpack();
            hasher.update(&(first_witness.len() as u64).to_le_bytes());
            hasher.update(&first_witness);
            // hash the other witnesses in the group
            for &index in &entry.indexes[1..] {
                let witness: Bytes = tx.witnesses().get(index).expect("get witness").unpack();
                hasher.update(&(witness.len() as u64).to_le_bytes());
                hasher.update(&witness);
            }
            // hash witnesses which do not in any input group
            for index in tx.raw().inputs().len()..tx.witnesses().len() {
                let witness: Bytes = tx.witnesses().get(index).expect("get witness").unpack();
                hasher.update(&(witness.len() as u64).to_le_bytes());
                hasher.update(&witness);
            }
            let mut message = [0u8; 32];
            hasher.finalize(&mut message);
            // sign tx
            let signature = self.sign_message(message)?;
            signatures.push(signature);
        }
        // seal
        let sealed_tx = tx_skeleton.seal(&signature_entries, signatures)?;
        // check fee rate
        sealed_tx.check_fee_rate()?;
        Ok(sealed_tx.transaction)
    }
}
