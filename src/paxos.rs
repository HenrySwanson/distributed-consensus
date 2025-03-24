mod multipaxos;
mod single_decree;

pub use multipaxos::MultiPaxos;
pub use single_decree::Paxos;

const ENABLE_NACKS: bool = true;
const PROPOSAL_COOLDOWN: u64 = 10;
const PROPOSAL_PROBABILITY: f64 = 0.05;
