mod multipaxos;
mod naive_multipaxos;
mod single_decree;

pub use multipaxos::MultiPaxos;
pub use naive_multipaxos::NaiveMultiPaxos;
pub use single_decree::SingleDecree;

use crate::simulation::ProcessID;

const ENABLE_NACKS: bool = true;
const PROPOSAL_COOLDOWN: u64 = 10;
const PROPOSAL_PROBABILITY: f64 = 0.05;

const TARGET_LOG_SIZE: usize = 10;
// TODO: if this is different it messes with the Consensus decision
const MAX_LOG_SIZE: usize = TARGET_LOG_SIZE;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProposalID(usize, ProcessID);

// utility function
fn get_mut_extending_if_necessary<T>(
    log: &mut Vec<T>,
    idx: usize,
    default: impl Fn() -> T,
) -> &mut T {
    if idx >= log.len() {
        log.resize_with(idx + 1, default);
    }
    log.get_mut(idx).unwrap()
}
