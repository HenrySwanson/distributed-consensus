use clap::Parser;

mod paxos;
mod simulation;

// TODO: push down into sim
const F: usize = 2;
const N: usize = 2 * F + 1;

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

    if args.stress {
        for n in 0..u64::MAX {
            let seed = rand::random();
            let mut sim = simulation::Simulation::from_seed(seed);
            let success = sim.run();
            if success {
                log::info!("Simulation {n} succeeded for seed {seed}")
            } else {
                log::error!("Simulation {n} did not succeed for seed {seed}");
                break;
            }
        }
    } else {
        let seed = args.seed.unwrap_or_else(rand::random);
        let mut sim = simulation::Simulation::from_seed(seed);
        let success = sim.run();

        log::info!("Seed was: {seed}");
        if !success {
            log::error!("Simulation did not succeed!");
        }
    }
}
