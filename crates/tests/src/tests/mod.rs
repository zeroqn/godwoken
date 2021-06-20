mod chain;
mod deposit_withdrawal;

#[derive(Serialized, Deserialized, Debug)]
pub enum ChallengeType {
    TxExecution,
    TxSignature,
    WithdrawalSignature,
}

#[derive(Serialized, Deserialized, Debug)]
pub enum ChaosAction {
    None,
    BreakBlock {
        target_index: u32,
        target_type: ChallengeType,
    },
    Challenge {
        block_number: u64,
        target_index: u32,
        target_type: ChallengeType,
    },
    WaitForChallengeMaturity,
}

#[derive(Serialized, Deserialized, Debug)]
pub enum Chaos {
    Enable,
    Disable,
    NextGlobalState(ChaosAction),
}

#[test]
fn test_deserialize() {
    let action = ChaosAction::Challenge {
        block_number: 32,
        target_index: 11,
        target_type: ChallengeType::TxExecution,
    };

    let chaos = Chaos::NextGlobalState(action);
    println!("serialized {}", serde_json::to_string(&chaos).unwrap());
}
