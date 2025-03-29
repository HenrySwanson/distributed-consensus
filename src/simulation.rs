use std::ops::ControlFlow;

use network::NetworkSettings;
use rand::distr::Uniform;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

use self::network::Network;
use crate::N;
use crate::QUORUM;

mod network;
mod process;

const MAX_TICKS: u64 = 10000;
const LOSS_PROBABILITY: f64 = 0.05;
const REPLAY_PROBABILITY: f64 = 0.05;
const MIN_NETWORK_DELAY: u64 = 3;
const MAX_NETWORK_DELAY: u64 = 10;
const CRASH_PROBABILITY: f64 = 0.05;
const UNCRASH_PROBABILITY: f64 = 0.2;

pub use network::Incoming;
pub use network::Outgoing;
pub use process::Merge;
pub use process::Process;
pub use process::ProcessID;

pub struct Simulation<P: Process> {
    clock: u64,
    state: [ProcessState<P>; N],
    network: Network<P::Message>,
    rng: StdRng,
}

// TODO: give these content again?
#[derive(Debug, Clone)]
pub enum Consensus {
    None,
    Partial,
    Complete,
    Conflict,
}

// TODO: take Process type instead?
pub struct Context<'sim, M> {
    pub current_tick: u64,
    pub rng: &'sim mut StdRng,
    // TODO: parcel these two into a single network object?
    pub received_messages: Vec<Incoming<M>>,
    pub outgoing_messages: &'sim mut Vec<Outgoing<M>>,
}

#[derive(Debug)]
pub struct Stats {
    pub ticks_elapsed: u64,
    pub num_messages_sent: u64,
}

struct ProcessState<P> {
    process: P,
    is_down: bool,
}

impl<P: Process> Simulation<P> {
    pub fn from_seed(seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        Self {
            clock: 0,
            state: std::array::from_fn(|id| ProcessState {
                process: P::new(ProcessID(id)),
                is_down: false,
            }),
            network: Network::new(
                // TODO: okay to use StdRng for parent and child?
                StdRng::from_rng(&mut rng),
                // TOOD: take this from constructor as well
                NetworkSettings {
                    loss_probability: LOSS_PROBABILITY,
                    replay_probability: REPLAY_PROBABILITY,
                    delay_distribution: Uniform::new_inclusive(
                        MIN_NETWORK_DELAY,
                        MAX_NETWORK_DELAY,
                    )
                    .expect("range error"),
                },
            ),
            rng,
        }
    }

    pub fn tick(&mut self) -> ControlFlow<(), ()> {
        self.clock += 1;
        log::trace!("==== TICK {:04} ====", self.clock);
        log::trace!("{} messages pending...", self.network.len());

        // Are we done?
        if self.state.iter().all(|p| p.process.is_done()) {
            log::trace!("Everyone has decided on a value!");
            return ControlFlow::Break(());
        }

        // Randomly crash and uncrash
        for idx in 0..self.state.len() {
            let down = self.state[idx].is_down;
            // always keep a quorum alive
            if !down && self.rng.random_bool(CRASH_PROBABILITY) {
                if self.state.iter().filter(|p| !p.is_down).count() > QUORUM {
                    log::trace!("Process {} is crashing!", idx);
                    self.state[idx].process.crash();
                    self.state[idx].is_down = true;
                }
            } else if down && self.rng.random_bool(UNCRASH_PROBABILITY) {
                log::trace!("Process {} is back up!", idx);
                self.state[idx].is_down = false;
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
            self.state[idx].process.tick(Context {
                current_tick: self.clock,
                rng: &mut self.rng,
                received_messages: messages,
                outgoing_messages: &mut replies,
            });
            self.network.enqueue(self.clock, ProcessID(idx), replies);
        }

        // Print current status
        for p in &self.state {
            log::trace!("{}", p.process.status());
        }
        log::trace!("====================");

        ControlFlow::Continue(())
    }

    pub fn run(&mut self) -> Consensus {
        for _ in 0..MAX_TICKS {
            match self.tick() {
                ControlFlow::Continue(_) => {}
                ControlFlow::Break(_) => break,
            }
        }

        log::trace!("======== END OF SIMULATION ========");
        for (i, p) in self.state.iter().enumerate() {
            log::trace!(
                "Process {} has decided on value {:?}",
                i,
                p.process.decided_value()
            )
        }

        // Check whether all processes are in a consistent state. It's okay
        // for some to have decided on a value and others not, but we can't
        // have two processes deciding on two different values.
        let values: Vec<_> = self
            .state
            .iter()
            .map(|p| p.process.decided_value())
            .collect();
        let merged = values
            .iter()
            .cloned()
            .try_fold(Merge::empty(), Merge::merge);
        match merged {
            Ok(merged) => {
                // okay, lots of possibilities here
                if merged == Merge::empty() {
                    Consensus::None
                } else if values.iter().all(|v| *v == merged) {
                    Consensus::Complete
                } else {
                    Consensus::Partial
                }
            }
            Err(_) => Consensus::Conflict,
        }
    }

    pub fn stats(&self) -> Stats {
        Stats {
            ticks_elapsed: self.clock,
            num_messages_sent: self.network.num_messages_sent(),
        }
    }
}

impl Stats {
    pub fn new() -> Self {
        Self {
            ticks_elapsed: 0,
            num_messages_sent: 0,
        }
    }

    pub fn merge(&mut self, other: Self) {
        self.ticks_elapsed += other.ticks_elapsed;
        self.num_messages_sent += other.num_messages_sent;
    }
}
