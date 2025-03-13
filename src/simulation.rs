use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

use self::network::Network;
use crate::paxos::Process;
use crate::N;

mod network;

const MAX_TICKS: u64 = 10000;
const LOSS_PROBABILITY: f64 = 0.1;
const MIN_NETWORK_DELAY: u64 = 3;
const MAX_NETWORK_DELAY: u64 = 10;

// TODO: move to paxos
const PROPOSAL_PROBABILITY: f64 = 0.05;

pub use network::AddressedMessage;

pub struct Simulation {
    clock: u64,
    processes: [Process; N],
    network: Network,
    rng: StdRng,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProcessID(pub usize);

impl Simulation {
    pub fn new() -> Self {
        let mut rng = StdRng::from_os_rng();
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

    pub fn run(&mut self) {
        for _ in 0..MAX_TICKS {
            self.clock += 1;
            println!("==== TICK {:04} ====", self.clock);
            println!("{} messages pending...", self.network.len());

            // Are we done?
            if self.processes.iter().all(|p| p.decided_value().is_some()) {
                println!("Everyone has decided on a value!");
                break;
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

impl std::fmt::Display for ProcessID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
