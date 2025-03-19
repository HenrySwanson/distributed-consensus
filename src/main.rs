use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use clap::Parser;
use paxos::Paxos;
use simulation::Consensus;

mod paxos;
mod simulation;

// TODO: push down into sim
const F: usize = 2;
const N: usize = 2 * F + 1;

// TODO:
// - Initial settings (contains N, F, NACKS_ENABLED, etc)
// - Logger that knows which process, which tick it is, verbose/not-verbose, etc.
// - Generalized consensus type (e.g. different for an RSM vs simple leader election)
// - Clock skew?
// - Formalize storage and crashing?
// - Figure out how to cleanly handle assertion failures and not lose the seed
// - Separate out P, A, L in Paxos
// - Add "stats" like number of messages sent
// - Network
//   - Do we distinguish UDP-like and TCP-like messages? (requires timeout/failure/retry)
//   - Add some kind of assertions on the message history? Tap-and-reduce, basically.
// - Property testing
//   - Allow "events" to happen from outside (e.g., network partition, crash) and test
//     over sequences of these events.

#[derive(Parser, Debug)]
struct Args {
    /// Seed to use for the random number generation. Should result in reproducible
    /// simulations.
    seed: Option<u64>,
    /// If set, all logging events are printed to stdout. Useful for debugging,
    /// but pretty noisy.
    #[arg(short, long)]
    verbose: bool,
    /// Runs repeatedly until failure
    #[arg(long)]
    stress: bool,
}

fn main() {
    let args = Args::parse();

    // TODO: implement your own logger specialized for simulated processes?
    // maybe, but for now this works fine.
    simple_logger::SimpleLogger::new()
        .with_level(match args.verbose {
            true => log::LevelFilter::Trace,
            false => log::LevelFilter::Info,
        })
        .without_timestamps()
        .init()
        .unwrap();

    let stop_signal = Arc::new(AtomicBool::new(false));

    ctrlc::set_handler({
        let stop_signal = stop_signal.clone();
        move || {
            println!("received Ctrl+C!");
            let already_stopped = stop_signal.swap(true, Ordering::Relaxed);
            if already_stopped {
                std::process::exit(1);
            }
        }
    })
    .expect("Error setting Ctrl-C handler");

    if args.stress {
        let mut successes = 0;
        let mut incompletes = 0;
        let mut failures = 0;

        for n in 0..u64::MAX {
            let seed = rand::random();
            // log::info!("Seed is: {seed}");

            let mut sim = simulation::Simulation::<Paxos>::from_seed(seed);
            let consensus = sim.run();

            match consensus {
                Consensus::None | Consensus::Partial => {
                    log::warn!("Simulation {n} did not complete for seed {seed}");
                    incompletes += 1;
                }
                Consensus::Complete => {
                    // log::info!("Simulation {n} did succeeded for seed {seed}");
                    successes += 1;
                }
                Consensus::Conflict => {
                    log::error!("Simulation {n} did not succeed for seed {seed}");
                    failures += 1;
                }
            }

            if stop_signal.load(Ordering::Relaxed) {
                break;
            }
        }

        println!(
            "Stress test completed: {} succeeded, {} incomplete, {} failed",
            successes, incompletes, failures
        );
    } else {
        let seed = args.seed.unwrap_or_else(rand::random);
        let mut sim = simulation::Simulation::<Paxos>::from_seed(seed);
        let consensus = sim.run();

        log::info!("Seed was: {seed}");
        if let Consensus::Complete = consensus {
        } else {
            log::error!("Simulation did not succeed!");
        }
    }
}
