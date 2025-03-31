use std::panic::UnwindSafe;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use clap::Parser;
use paxos::MultiPaxos;
use paxos::NaiveMultiPaxos;
use paxos::SingleDecree;
use scenario::easy_scenario;
use scenario::everything_scenario;
use simulation::Consensus;
use simulation::Process;
use simulation::Simulation;
use simulation::Stats;

mod paxos;
mod scenario;
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
    /// Which algorithm to test
    #[arg(long)]
    algorithm: Option<Algorithm>,
    /// Which scenario to run
    #[arg(long)]
    scenario: Option<Scenario>,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum Algorithm {
    SingleDecree,
    NaiveMulti,
    Multi,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum Scenario {
    Easy,
    Everything,
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
    let scenario = args.scenario.unwrap_or(Scenario::Everything);

    // TODO: seems like there should be a better way to do this
    match algorithm {
        Algorithm::SingleDecree => do_test::<SingleDecree>(args.seed, args.stress, scenario),
        Algorithm::NaiveMulti => do_test::<NaiveMultiPaxos>(args.seed, args.stress, scenario),
        Algorithm::Multi => do_test::<MultiPaxos>(args.seed, args.stress, scenario),
    }
}

fn do_test<P>(seed: Option<u64>, stress: bool, scenario: Scenario)
where
    P: Process + UnwindSafe,
    P::Message: UnwindSafe,
{
    if stress {
        stress_test::<P>(scenario)
    } else {
        let seed = seed.unwrap_or_else(rand::random);
        run_once::<P>(seed, scenario)
    }
}

fn stress_test<P>(scenario: Scenario)
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

        let sim = match std::panic::catch_unwind(|| get_scenario::<P>(seed, scenario)) {
            Ok(x) => x,
            Err(panic) => {
                // log the failing seed and then resume unwinding
                log::info!("Seed was: {seed}");
                std::panic::resume_unwind(panic)
            }
        };

        let consensus = sim.check_consensus();
        let stats = sim.stats();
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

fn run_once<P>(seed: u64, scenario: Scenario)
where
    P: Process,
{
    let sim: Simulation<P> = get_scenario(seed, scenario);

    log::info!("Seed was: {seed}");
    if let Consensus::Complete = sim.check_consensus() {
    } else {
        log::error!("Simulation did not succeed!");
    }

    println!("{:?}", sim.stats());
}

fn get_scenario<P>(seed: u64, scenario: Scenario) -> Simulation<P>
where
    P: Process,
{
    match scenario {
        Scenario::Easy => easy_scenario(seed),
        Scenario::Everything => everything_scenario(seed),
    }
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
