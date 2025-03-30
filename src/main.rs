use std::panic::UnwindSafe;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use clap::Parser;
use paxos::MultiPaxos;
use paxos::NaiveMultiPaxos;
use paxos::SingleDecree;
use simulation::Consensus;
use simulation::Process;
use simulation::Stats;

mod paxos;
mod simulation;

// TODO: push down into sim
const N: usize = 5;
const QUORUM: usize = N / 2 + 1;

// TODO:
// - Initial settings (contains N, F, NACKS_ENABLED, etc)
// - Logger that knows which process, which tick it is, verbose/not-verbose, etc.
// - Clock skew?
// - Formalize storage?
// - Separate crashing and uncrashing (and pass current time to fix the timer issue)
// - Create timer class?
// - Separate out P, A, L in Paxos
// - Network
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
    #[arg(long)]
    algorithm: Option<Algorithm>,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum Algorithm {
    SingleDecree,
    NaiveMulti,
    Multi,
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

    let algorithm = args.algorithm.unwrap_or(Algorithm::Multi);

    if args.stress {
        match algorithm {
            Algorithm::SingleDecree => stress_test::<SingleDecree>(),
            Algorithm::NaiveMulti => stress_test::<NaiveMultiPaxos>(),
            Algorithm::Multi => stress_test::<MultiPaxos>(),
        }
    } else {
        let seed = args.seed.unwrap_or_else(rand::random);
        match algorithm {
            Algorithm::SingleDecree => run_once::<SingleDecree>(seed),
            Algorithm::NaiveMulti => run_once::<NaiveMultiPaxos>(seed),
            Algorithm::Multi => run_once::<MultiPaxos>(seed),
        }
    }
}

fn stress_test<P>()
where
    P: Process + UnwindSafe,
    P::Message: UnwindSafe,
{
    let stop_signal = configure_ctrlc_handler();

    let mut successes = 0;
    let mut incompletes = 0;
    let mut failures = 0;

    let mut total_stats = Stats::new();

    for n in 0..u64::MAX {
        let seed = rand::random();

        let mut sim = simulation::Simulation::<P>::from_seed(seed);
        let (consensus, stats) = match std::panic::catch_unwind(move || {
            let consensus = sim.run();
            (consensus, sim.stats())
        }) {
            Ok(x) => x,
            Err(panic) => {
                // log the failing seed and then resume unwinding
                log::info!("Seed was: {seed}");
                std::panic::resume_unwind(panic)
            }
        };

        total_stats.merge(stats);

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
    let total = (successes + incompletes + failures) as f64;
    println!(
        "Statistics: {:.2} average ticks to completion, {:.2} average messages sent",
        total_stats.ticks_elapsed as f64 / total,
        total_stats.num_messages_sent as f64 / total
    )
}

fn run_once<P>(seed: u64)
where
    P: Process,
{
    let mut sim = simulation::Simulation::<P>::from_seed(seed);
    let consensus = sim.run();

    log::info!("Seed was: {seed}");
    if let Consensus::Complete = consensus {
    } else {
        log::error!("Simulation did not succeed!");
    }

    println!("{:?}", sim.stats());
}

fn configure_ctrlc_handler() -> Arc<AtomicBool> {
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
    stop_signal
}
