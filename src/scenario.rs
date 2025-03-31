use rand::distr::Uniform;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

use crate::simulation::NetworkSettings;
use crate::simulation::Process;
use crate::simulation::Simulation;
use crate::N;
use crate::QUORUM;

// TODO: put in CLI args?
const MAX_TICKS: u64 = 10000;
const LIVELOCK_MODE_THRESHOLD: u64 = MAX_TICKS * 4 / 5;
const CRASH_PROBABILITY: f64 = 0.05;
const UNCRASH_PROBABILITY: f64 = 0.2;

const LOSS_PROBABILITY: f64 = 0.05;
const REPLAY_PROBABILITY: f64 = 0.05;
const MIN_NETWORK_DELAY: u64 = 3;
const MAX_NETWORK_DELAY: u64 = 10;

/// Nothing bad happens; no one crashes, no partitions, not even packet duplication.
pub fn easy_scenario<P: Process>(seed: u64) -> Simulation<P> {
    // Generate the master RNG
    let mut master_rng = StdRng::seed_from_u64(seed);
    let mut sim = Simulation::<P>::new(
        StdRng::from_rng(&mut master_rng),
        NetworkSettings {
            loss_probability: 0.0,
            replay_probability: 0.0,
            delay_distribution: Uniform::new_inclusive(MIN_NETWORK_DELAY, MAX_NETWORK_DELAY)
                .expect("range error"),
        },
    );

    for _ in 0..MAX_TICKS {
        // Are we done?
        if sim.processes().iter().all(|p| p.process.is_done()) {
            log::trace!("Everyone has decided on a value!");
            break;
        }

        // Tick once
        sim.tick();
    }

    log::trace!("======== END OF SIMULATION ========");
    for (i, p) in sim.processes().iter().enumerate() {
        log::trace!(
            "Process {} has decided on value {:?}",
            i,
            p.process.decided_value()
        )
    }

    sim
}

/// Throws a little bit of everything at the processes: crashes, packet loss,
/// packet duplication, you name it.
pub fn everything_scenario<P: Process>(seed: u64) -> Simulation<P> {
    // Generate the master RNG
    let mut master_rng = StdRng::seed_from_u64(seed);
    let mut sim = Simulation::<P>::new(
        StdRng::from_rng(&mut master_rng),
        NetworkSettings {
            loss_probability: LOSS_PROBABILITY,
            replay_probability: REPLAY_PROBABILITY,
            delay_distribution: Uniform::new_inclusive(MIN_NETWORK_DELAY, MAX_NETWORK_DELAY)
                .expect("range error"),
        },
    );

    for t in 0..MAX_TICKS {
        // Are we done?
        if sim.processes().iter().all(|p| p.process.is_done()) {
            log::trace!("Everyone has decided on a value!");
            break;
        }

        // Randomly crash and uncrash
        let enforce_quorum = t > LIVELOCK_MODE_THRESHOLD;
        for idx in 0..N {
            let down = sim.get_process(idx).is_down;
            if !down && master_rng.random_bool(CRASH_PROBABILITY) {
                let live_count = sim.processes().iter().filter(|p| !p.is_down).count();
                if !enforce_quorum || live_count > QUORUM {
                    sim.crash(idx);
                }
            } else if down && master_rng.random_bool(UNCRASH_PROBABILITY) {
                sim.uncrash(idx);
            }
        }

        // Tick once
        sim.tick();
    }

    log::trace!("======== END OF SIMULATION ========");
    for (i, p) in sim.processes().iter().enumerate() {
        log::trace!(
            "Process {} has decided on value {:?}",
            i,
            p.process.decided_value()
        )
    }

    sim
}
