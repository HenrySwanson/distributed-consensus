use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::paxos::Message;
use crate::ProcessID;
use crate::NETWORK_DELAY;

#[derive(Debug)]
pub struct Network {
    in_flight: BinaryHeap<Reverse<Packet>>,
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
    pub fn new() -> Self {
        Self {
            in_flight: BinaryHeap::new(),
        }
    }

    pub fn enqueue(&mut self, current_tick: u64, msgs: Vec<AddressedMessage>) {
        println!("Sending {} messages:", msgs.len());
        for msg in &msgs {
            println!("  {} to {}: {:?}", msg.from.0, msg.to.0, msg.msg);
        }

        let arrival_time = current_tick + NETWORK_DELAY;
        self.in_flight.extend(
            msgs.into_iter()
                .map(|msg| Reverse(Packet { arrival_time, msg })),
        );
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

    /// delays the upcoming message
    /// todo: implement this as part of arrival time
    pub fn delay(&mut self) {
        let Some(mut top) = self.in_flight.peek_mut() else {
            return;
        };
        println!("Delaying message {:?}", top.0.msg);
        top.0.arrival_time += 2;
    }

    /// drops the first message
    pub fn drop(&mut self) {
        if let Some(Reverse(msg)) = self.in_flight.pop() {
            println!("Dropping message {:?}", msg.msg);
        }
    }
}
