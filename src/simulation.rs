use rand::rngs::StdRng;
use rand::SeedableRng;

use self::network::Network;
use crate::N;

mod network;
mod process;

pub use network::Incoming;
pub use network::NetworkSettings;
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

pub struct ProcessState<P> {
    pub process: P,
    pub is_down: bool,
}

impl<P: Process> Simulation<P> {
    pub fn new(mut rng: StdRng, network_settings: NetworkSettings) -> Self {
        Self {
            clock: 0,
            state: std::array::from_fn(|id| ProcessState {
                process: P::new(ProcessID(id)),
                is_down: false,
            }),
            network: Network::new(
                // TODO: okay to use StdRng for parent and child?
                StdRng::from_rng(&mut rng),
                network_settings,
            ),
            rng,
        }
    }

    pub fn get_process(&self, idx: usize) -> &ProcessState<P> {
        &self.state[idx]
    }

    pub fn processes(&self) -> &[ProcessState<P>] {
        &self.state
    }

    pub fn network_mut(&mut self) -> &mut Network<P::Message> {
        &mut self.network
    }

    pub fn tick(&mut self) {
        self.clock += 1;
        log::trace!("==== TICK {:04} ====", self.clock);
        log::trace!("{} messages pending...", self.network.len());

        // Fetch messages
        let mut msgs_to_deliver: [_; N] = std::array::from_fn(|_| vec![]);
        while let Some((msg, to)) = self.network.next_msg(self.clock) {
            msgs_to_deliver[to.0].push(msg);
        }

        // Tick each process, unless it is down
        for (idx, messages) in msgs_to_deliver.into_iter().enumerate() {
            if self.state[idx].is_down {
                continue;
            }

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
            log::trace!(
                "{}",
                if p.is_down {
                    "DOWN".to_string()
                } else {
                    p.process.status()
                }
            );
        }
        log::trace!("====================");
    }

    pub fn stats(&self) -> Stats {
        Stats {
            ticks_elapsed: self.clock,
            num_messages_sent: self.network.num_messages_sent(),
        }
    }

    /// Check whether all processes are in a consistent state. It's okay
    /// for some to have decided on a value and others not, but we can't
    /// have two processes deciding on two different values.
    pub fn check_consensus(&self) -> Consensus {
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

    pub fn crash(&mut self, idx: usize) {
        log::trace!("Process {} is crashing!", idx);
        self.state[idx].is_down = true;
    }

    pub fn uncrash(&mut self, idx: usize) {
        log::trace!("Process {} is back up!", idx);
        self.state[idx].is_down = false;
        self.state[idx].process.restore_from_crash(self.clock);
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
