use std::cmp::Reverse;
use std::collections::BinaryHeap;

use rand::distr::{Distribution, Uniform};
use rand::rngs::StdRng;
use rand::Rng;

use crate::paxos::Message;
use crate::ProcessID;

#[derive(Debug)]
pub struct Network {
    in_flight: BinaryHeap<Reverse<Packet>>,
    rng: StdRng,
    loss_probability: f64,
    delay_distribution: Uniform<u64>,
}

// TODO: generic over message?
#[derive(Debug, Clone)]
pub struct AddressedMessage {
    pub from: ProcessID,
    pub to: ProcessID,
    pub msg: Message,
}

#[derive(Debug, Clone)]
struct Packet {
    arrival_time: u64,
    msg: AddressedMessage,
}

impl PartialEq for Packet {
    fn eq(&self, other: &Self) -> bool {
        self.arrival_time == other.arrival_time
    }
}

impl Eq for Packet {}

impl PartialOrd for Packet {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Packet {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.arrival_time.cmp(&other.arrival_time)
    }
}

impl Network {
    pub fn new(rng: StdRng, loss_probability: f64, min_delay: u64, max_delay: u64) -> Self {
        assert!(min_delay <= max_delay);
        Self {
            in_flight: BinaryHeap::new(),
            rng,
            loss_probability,
            delay_distribution: Uniform::new_inclusive(min_delay, max_delay).unwrap(),
        }
    }

    pub fn enqueue(&mut self, current_tick: u64, msgs: Vec<AddressedMessage>) {
        println!("Sending {} messages:", msgs.len());
        for msg in &msgs {
            println!("  {} to {}: {:?}", msg.from.0, msg.to.0, msg.msg);
        }

        self.in_flight.extend(msgs.into_iter().filter_map(|msg| {
            // Q: do this at pop time or enqueue time?
            if self.rng.random_bool(self.loss_probability) {
                println!("Dropping message {:?}", msg.msg);
                return None;
            }

            let delay = self.delay_distribution.sample(&mut self.rng);
            Some(Reverse(Packet {
                arrival_time: current_tick + delay,
                msg,
            }))
        }));
    }

    pub fn next_msg(&mut self, current_tick: u64) -> Option<AddressedMessage> {
        if let Some(Reverse(packet)) = self.in_flight.peek() {
            if packet.arrival_time <= current_tick {
                println!(
                    "Received a message:  {} -> {}: {:?}",
                    packet.msg.from.0, packet.msg.to.0, packet.msg.msg
                );
                return self.in_flight.pop().map(|x| x.0.msg);
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
