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

fn main() {
    println!("Hello, world!");

    let mut sim = simulation::Simulation::new();
    sim.run();
}
