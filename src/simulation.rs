use itertools::Itertools;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

use self::network::Network;
use crate::paxos::Message;
use crate::paxos::Process;
use crate::F;
use crate::N;

mod network;

const MAX_TICKS: u64 = 10000;
const LOSS_PROBABILITY: f64 = 0.05;
const MIN_NETWORK_DELAY: u64 = 3;
const MAX_NETWORK_DELAY: u64 = 10;
const CRASH_PROBABILITY: f64 = 0.05;
const UNCRASH_PROBABILITY: f64 = 0.2;

pub use network::Incoming;
pub use network::Outgoing;

pub struct Simulation {
    clock: u64,
    processes: [Process; N],
    network: Network<Message>,
    rng: StdRng,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProcessID(pub usize);

pub struct Context<'sim> {
    pub current_tick: u64,
    pub rng: &'sim mut StdRng,
    // TODO: parcel these two into a single network object?
    pub received_messages: Vec<Incoming<Message>>,
    pub outgoing_messages: &'sim mut Vec<Outgoing<Message>>,
}

impl Simulation {
    pub fn from_seed(seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        Self {
            clock: 0,
            processes: std::array::from_fn(|id| Process::new(ProcessID(id))),
            network: Network::new(
                // TODO: okay to use StdRng for parent and child?
                StdRng::from_rng(&mut rng),
                LOSS_PROBABILITY,
                MIN_NETWORK_DELAY,
                MAX_NETWORK_DELAY,
            ),
            rng,
        }
    }

    pub fn run(&mut self) -> bool {
        let mut is_down: [_; N] = std::array::from_fn(|_| false);

        for _ in 0..MAX_TICKS {
            self.clock += 1;
            log::trace!("==== TICK {:04} ====", self.clock);
            log::trace!("{} messages pending...", self.network.len());

            // Are we done?
            if self.processes.iter().all(|p| p.decided_value().is_some()) {
                log::trace!("Everyone has decided on a value!");
                break;
            }

            // Randomly crash and uncrash
            for idx in 0..N {
                let down = is_down[idx];
                // never bring down more than a quorum!
                if !down && self.rng.random_bool(CRASH_PROBABILITY) {
                    if is_down.iter().filter(|x| **x).count() < F {
                        log::trace!("Process {} is crashing!", idx);
                        self.processes[idx].crash();
                        is_down[idx] = true;
                    }
                } else if down && self.rng.random_bool(UNCRASH_PROBABILITY) {
                    log::trace!("Process {} is back up!", idx);
                    is_down[idx] = false;
                }
            }

            // Fetch messages
            let mut msgs_to_deliver: [_; N] = std::array::from_fn(|_| vec![]);
            while let Some((msg, to)) = self.network.next_msg(self.clock) {
                msgs_to_deliver[to.0].push(msg);
            }

            // Tick each process
            for (idx, messages) in msgs_to_deliver.into_iter().enumerate() {
                let mut replies = vec![];
                self.processes[idx].tick(Context {
                    current_tick: self.clock,
                    rng: &mut self.rng,
                    received_messages: messages,
                    outgoing_messages: &mut replies,
                });
                self.network.enqueue(self.clock, ProcessID(idx), replies);
            }

            // Print current status
            for p in &self.processes {
                log::trace!("{}", p.status());
            }
            log::trace!("====================");
        }

        log::trace!("======== END OF SIMULATION ========");
        for p in &self.processes {
            log::trace!(
                "Process {} has decided on value {}",
                p.id.0,
                p.decided_value().as_ref().map_or("NONE", |s| s.as_str())
            )
        }

        // Check whether all processes are in a consistent state. It's okay
        // for some to have decided on a value and others not, but we can't
        // have two processes deciding on two different values.
        self.processes
            .iter()
            .filter_map(|p| p.decided_value())
            .all_equal()
    }
}

impl std::fmt::Display for ProcessID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
