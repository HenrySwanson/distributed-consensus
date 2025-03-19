use std::cmp::Reverse;
use std::collections::BinaryHeap;

use rand::distr::Distribution;
use rand::distr::Uniform;
use rand::rngs::StdRng;
use rand::Rng;

use super::ProcessID;

#[derive(Debug)]
pub struct Network<M> {
    in_flight: BinaryHeap<Reverse<Packet<M>>>,
    rng: StdRng,
    settings: NetworkSettings,
}

#[derive(Debug, Clone)]
pub struct NetworkSettings {
    pub loss_probability: f64,
    pub replay_probability: f64,
    pub delay_distribution: Uniform<u64>,
}

#[derive(Debug, Clone)]
pub struct Incoming<M> {
    pub from: ProcessID,
    pub msg: M,
}

#[derive(Debug, Clone)]
pub struct Outgoing<M> {
    pub to: ProcessID,
    pub msg: M,
}

#[derive(Debug, Clone)]
struct Packet<M> {
    arrival_time: u64,
    msg: M,
    from: ProcessID,
    to: ProcessID,
}

impl<M> PartialEq for Packet<M> {
    fn eq(&self, other: &Self) -> bool {
        self.arrival_time == other.arrival_time
    }
}

impl<M> Eq for Packet<M> {}

impl<M> PartialOrd for Packet<M> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<M> Ord for Packet<M> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.arrival_time.cmp(&other.arrival_time)
    }
}

impl<M> Network<M> {
    pub fn new(rng: StdRng, settings: NetworkSettings) -> Self {
        assert!(settings.loss_probability >= 0.0);
        assert!(settings.loss_probability <= 1.0);
        assert!(settings.replay_probability >= 0.0);
        assert!(settings.replay_probability <= 1.0);

        Self {
            in_flight: BinaryHeap::new(),
            rng,
            settings,
        }
    }

    pub fn enqueue(&mut self, current_tick: u64, from: ProcessID, msgs: Vec<Outgoing<M>>)
    where
        M: std::fmt::Debug,
    {
        log::trace!("Sending {} messages:", msgs.len());
        for msg in &msgs {
            log::trace!("  {} to {}: {:?}", from, msg.to, msg.msg);
        }

        self.in_flight
            .extend(msgs.into_iter().filter_map(|out_msg| {
                // Q: do this at pop time or enqueue time?
                if self.rng.random_bool(self.settings.loss_probability) {
                    log::trace!("Dropping message {:?}", out_msg.msg);
                    return None;
                }

                let delay = self.settings.delay_distribution.sample(&mut self.rng);
                Some(Reverse(Packet {
                    arrival_time: current_tick + delay,
                    msg: out_msg.msg,
                    from,
                    to: out_msg.to,
                }))
            }));
    }

    pub fn next_msg(&mut self, current_tick: u64) -> Option<(Incoming<M>, ProcessID)>
    where
        M: std::fmt::Debug + Clone,
    {
        if let Some(Reverse(packet)) = self.in_flight.peek() {
            if packet.arrival_time <= current_tick {
                log::trace!(
                    "Received a message:  {} -> {}: {:?}",
                    packet.from,
                    packet.to,
                    packet.msg
                );

                let Reverse(packet) = self.in_flight.pop().unwrap();

                if self.rng.random_bool(self.settings.replay_probability) {
                    log::trace!(
                        "Replaying a message:  {} -> {}: {:?}",
                        packet.from,
                        packet.to,
                        packet.msg
                    );
                    // TODO: is there a better way to do this?
                    self.enqueue(
                        current_tick,
                        packet.from,
                        vec![Outgoing {
                            to: packet.to,
                            msg: packet.msg.clone(),
                        }],
                    );
                }

                self.in_flight.pop().map(|x| {
                    let Packet { msg, from, to, .. } = x.0;
                    (Incoming { msg, from }, to)
                });
            }
        }
        None
    }

    pub fn is_empty(&self) -> bool {
        self.in_flight.is_empty()
    }

    pub fn len(&self) -> usize {
        self.in_flight.len()
    }
}

impl Default for NetworkSettings {
    fn default() -> Self {
        Self {
            loss_probability: 0.0,
            replay_probability: 0.0,
            delay_distribution: Uniform::new_inclusive(0, 0).expect("range error"),
        }
    }
}
