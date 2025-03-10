use std::fmt::Display;

use network::Network;
use paxos::Process;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

mod network;
mod paxos;

const F: usize = 2;
const N: usize = 2 * F + 1;

const MAX_TICKS: u64 = 10000;
const LOSS_PROBABILITY: f64 = 0.1;
const DELAY_PROBABILITY: f64 = 0.2;
const ENABLE_NACKS: bool = true;
const NETWORK_DELAY: u64 = 3;
const PROPOSAL_PROBABILITY: f64 = 0.05;
const PROPOSAL_COOLDOWN: u64 = 10;

// TODO:
// - Some kind of log that isn't stdout
// - Deterministic replay
//   - Could start over from the beginning, or rewind to snapshot and replay.
//   - Latter seems harder, but we could do it with something like StdRng that can
//     be cloned.
// - Timeouts
//   - Introduce global clock first, then allow for skew later
//   - For rewind-and-replay, use StdRng so we can clone it as part of the saved state
// - Network
//   - Implement message duplication
//   - Do we distinguish UDP-like and TCP-like messages? (requires timeout/failure/retry)
// - Other
//   - Should we use async to implement these?
//   - Implement crashing!

fn main() {
    println!("Hello, world!");

    let mut sim = Simulation::new();
    sim.run();
}

struct Simulation {
    clock: u64,
    processes: [Process; N],
    network: Network,
    rng: StdRng,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ProcessID(usize);

impl Simulation {
    fn new() -> Self {
        Self {
            clock: 0,
            processes: std::array::from_fn(|id| Process::new(ProcessID(id))),
            network: Network::new(),
            rng: StdRng::from_os_rng(),
        }
    }

    fn run(&mut self) {
        for _ in 0..MAX_TICKS {
            self.clock += 1;
            println!("==== TICK {:04} ====", self.clock);
            println!("{} messages pending...", self.network.len());

            // Are we done?
            if self.processes.iter().all(|p| p.decided_value().is_some()) {
                println!("Everyone has decided on a value!");
                break;
            }

            // With low probability, drop a message
            if !self.network.is_empty() && self.rng.random_bool(LOSS_PROBABILITY) {
                self.network.drop();
            }
            // With slightly higher probability, delay a message
            if self.network.len() >= 2 && self.rng.random_bool(DELAY_PROBABILITY) {
                self.network.delay();
            }
            // For each eligible process, check if it timed out and issued a new proposal
            for p in &mut self.processes {
                if p.decided_value().is_none()
                    && p.next_timeout_at() <= self.clock
                    && self.rng.random_bool(PROPOSAL_PROBABILITY)
                {
                    println!("Random proposal from {}!", p.id.0);
                    let proposal_msgs = p.create_proposal_messages(self.clock);
                    self.network.enqueue(self.clock, proposal_msgs);
                }
            }

            // Get the next packet and act on it
            if let Some(msg) = self.network.next_msg(self.clock) {
                let replies = self.processes[msg.to.0].recv_message(msg, self.clock);
                self.network.enqueue(self.clock, replies);
            };

            // Print current status
            for p in &self.processes {
                println!("{}", p.status());
            }
            println!("====================");
        }

        println!("======== END OF SIMULATION ========");
        for p in &self.processes {
            println!(
                "Process {} has decided on value {}",
                p.id.0,
                p.decided_value().as_ref().map_or("NONE", |s| s.as_str())
            )
        }
        if self
            .processes
            .iter()
            .all(|p| p.decided_value() == self.processes[0].decided_value())
        {
            println!("SUCCESS!");
        } else {
            println!("FAILURE!");
        }
    }
}

impl Display for ProcessID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
