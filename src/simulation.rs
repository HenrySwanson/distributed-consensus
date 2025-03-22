use std::ops::ControlFlow;

use itertools::Itertools;
use network::NetworkSettings;
use rand::distr::Uniform;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

use self::network::Network;
use crate::F;
use crate::N;

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
        if self
            .state
            .iter()
            .all(|p| p.process.decided_value().is_some())
        {
            log::trace!("Everyone has decided on a value!");
            return ControlFlow::Break(());
        }

        // Randomly crash and uncrash
        for idx in 0..self.state.len() {
            let down = self.state[idx].is_down;
            // never bring down more than a quorum!
            if !down && self.rng.random_bool(CRASH_PROBABILITY) {
                if self.state.iter().filter(|p| p.is_down).count() < F {
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
                "Process {} has decided on value {}",
                i,
                p.process
                    .decided_value()
                    .as_ref()
                    .map_or("NONE", |s| s.as_str())
            )
        }

        // Check whether all processes are in a consistent state. It's okay
        // for some to have decided on a value and others not, but we can't
        // have two processes deciding on two different values.
        match self
            .state
            .iter()
            .filter_map(|p| p.process.decided_value())
            .all_equal_value()
        {
            Ok(_) => {
                // could be Complete or Partial
                if self
                    .state
                    .iter()
                    .all(|p| p.process.decided_value().is_some())
                {
                    Consensus::Complete
                } else {
                    Consensus::Partial
                }
            }
            Err(Some(_)) => Consensus::Conflict,
            Err(None) => Consensus::None,
        }
    }
}
