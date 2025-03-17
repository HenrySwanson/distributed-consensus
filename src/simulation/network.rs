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
    loss_probability: f64,
    delay_distribution: Uniform<u64>,
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
    pub fn new(rng: StdRng, loss_probability: f64, min_delay: u64, max_delay: u64) -> Self {
        assert!(min_delay <= max_delay);
        Self {
            in_flight: BinaryHeap::new(),
            rng,
            loss_probability,
            delay_distribution: Uniform::new_inclusive(min_delay, max_delay).unwrap(),
        }
    }

    pub fn enqueue(&mut self, current_tick: u64, from: ProcessID, msgs: Vec<Outgoing<M>>)
    where
        M: std::fmt::Debug,
    {
        println!("Sending {} messages:", msgs.len());
        for msg in &msgs {
            println!("  {} to {}: {:?}", from, msg.to, msg.msg);
        }

        self.in_flight
            .extend(msgs.into_iter().filter_map(|out_msg| {
                // Q: do this at pop time or enqueue time?
                if self.rng.random_bool(self.loss_probability) {
                    println!("Dropping message {:?}", out_msg.msg);
                    return None;
                }

                let delay = self.delay_distribution.sample(&mut self.rng);
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
        M: std::fmt::Debug,
    {
        if let Some(Reverse(packet)) = self.in_flight.peek() {
            if packet.arrival_time <= current_tick {
                println!(
                    "Received a message:  {} -> {}: {:?}",
                    packet.from, packet.to, packet.msg
                );
                return self.in_flight.pop().map(|x| {
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
