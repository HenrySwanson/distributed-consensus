use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use clap::Parser;
use paxos::Paxos;

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
// - Return should be three-valued: complete consensus, partial consensus, broken
//   - This will let us detect livelock!
// - Ctrl-C to gently stop stress testing and give us a chance to print stats.
// - Network
//   - Implement message duplication
//   - Do we distinguish UDP-like and TCP-like messages? (requires timeout/failure/retry)
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
        for n in 0..u64::MAX {
            let seed = rand::random();
            let mut sim = simulation::Simulation::<Paxos>::from_seed(seed);
            let success = sim.run();
            if success {
                // log::info!("Simulation {n} succeeded for seed {seed}")
            } else {
                log::error!("Simulation {n} did not succeed for seed {seed}");
                break;
            }

            if stop_signal.load(Ordering::Relaxed) {
                break;
            }
        }

        println!("Stress test completed!")
    } else {
        let seed = args.seed.unwrap_or_else(rand::random);
        let mut sim = simulation::Simulation::<Paxos>::from_seed(seed);
        let success = sim.run();

        log::info!("Seed was: {seed}");
        if !success {
            log::error!("Simulation did not succeed!");
        }
    }
}
