use rand::rngs::StdRng;
use rand::SeedableRng;

use self::network::Network;
use crate::paxos::Process;
use crate::N;

mod network;

const MAX_TICKS: u64 = 10000;
const LOSS_PROBABILITY: f64 = 0.1;
const MIN_NETWORK_DELAY: u64 = 3;
const MAX_NETWORK_DELAY: u64 = 10;

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

            // Fetch messages
            let mut msgs_to_deliver: [_; N] = std::array::from_fn(|_| vec![]);
            while let Some(msg) = self.network.next_msg(self.clock) {
                msgs_to_deliver[msg.to.0].push(msg);
            }

            // Tick each process
            for (p, messages) in self.processes.iter_mut().zip(msgs_to_deliver) {
                let replies = p.tick(self.clock, messages, &mut self.rng);
                self.network.enqueue(self.clock, replies);
            }

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
