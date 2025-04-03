use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashSet;

use all_asserts::assert_range;
use rand::distr::Distribution;
use rand::distr::Uniform;
use rand::rngs::StdRng;
use rand::Rng;

use super::ProcessID;

#[derive(Debug)]
pub struct Network<M> {
    in_flight: BinaryHeap<Reverse<Packet<M>>>,
    partition: HashSet<ProcessID>, // inside and outside of set can't communicate
    // TODO: partial partitions? more-than-binary partitions?
    rng: StdRng,
    settings: NetworkSettings,
    num_messages_sent: u64,
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
        assert_range!(0.0..=1.0, settings.loss_probability);
        assert_range!(0.0..=1.0, settings.replay_probability);

        Self {
            in_flight: BinaryHeap::new(),
            partition: HashSet::new(),
            rng,
            settings,
            num_messages_sent: 0,
        }
    }

    pub fn enqueue(&mut self, current_tick: u64, from: ProcessID, msgs: Vec<Outgoing<M>>)
    where
        M: std::fmt::Debug,
    {
        for msg in &msgs {
            log::trace!("Sending a message:  {} -> {}: {:?}", from, msg.to, msg.msg);
            self.num_messages_sent += 1;
        }

        self.in_flight
            .extend(msgs.into_iter().filter_map(|Outgoing { to, msg }| {
                // Q: do this at pop time or enqueue time?
                if self.rng.random_bool(self.settings.loss_probability) {
                    log::trace!("Dropping message {:?}", msg);
                    return None;
                }

                if self.partition.contains(&from) != self.partition.contains(&to) {
                    log::trace!("Message {:?} blocked by partition", msg);
                    return None;
                }

                let delay = self.settings.delay_distribution.sample(&mut self.rng);
                Some(Reverse(Packet {
                    arrival_time: current_tick + delay,
                    msg,
                    from,
                    to,
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

                // remove the packet from the network (peek() tells us this succeeds always)
                let Reverse(Packet { msg, from, to, .. }) = self.in_flight.pop().unwrap();

                if self.rng.random_bool(self.settings.replay_probability) {
                    log::trace!("Replaying a message:  {} -> {}: {:?}", from, to, msg);
                    // TODO: is there a better way to do this?
                    self.enqueue(
                        current_tick,
                        from,
                        vec![Outgoing {
                            to,
                            msg: msg.clone(),
                        }],
                    );
                }

                return Some((Incoming { msg, from }, to));
            }
        }
        None
    }

    pub fn len(&self) -> usize {
        self.in_flight.len()
    }

    pub fn num_messages_sent(&self) -> u64 {
        self.num_messages_sent
    }

    pub fn create_partition(&mut self, nodes: impl IntoIterator<Item = ProcessID>) {
        self.partition = nodes.into_iter().collect()
    }

    pub fn clear_partition(&mut self) {
        self.partition.clear()
    }
}

impl<M: Clone> Outgoing<M> {
    pub fn broadcast(
        msg: M,
        recipients: impl Iterator<Item = ProcessID>,
    ) -> impl Iterator<Item = Self> {
        recipients.map(move |to| Outgoing {
            to,
            msg: msg.clone(),
        })
    }

    pub fn broadcast_everyone(msg: M) -> impl Iterator<Item = Self> {
        Outgoing::broadcast(msg, (0..crate::N).map(ProcessID))
    }

    pub fn broadcast_everyone_except(msg: M, id: ProcessID) -> impl Iterator<Item = Self> {
        Outgoing::broadcast(
            msg,
            (0..crate::N).filter(move |i| *i != id.0).map(ProcessID),
        )
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
