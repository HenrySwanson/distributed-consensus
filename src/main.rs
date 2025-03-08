use std::collections::HashMap;
use std::collections::VecDeque;

use rand::seq::IteratorRandom;
use rand::Rng;

const F: usize = 2;
const N: usize = 2 * F + 1;

const MAX_ROUNDS: usize = 10000;
const LOSS_PROBABILITY: f64 = 0.1;
const DELAY_PROBABILITY: f64 = 0.2;

fn main() {
    println!("Hello, world!");

    let mut processes: Vec<_> = (0..N).map(|i| Process::new(ProcessID(i))).collect();
    // TODO: introduce message delay, loss, duplication, reordering, etc
    let mut network = Network::new();
    let mut rng = rand::rng();

    for round_number in 0..MAX_ROUNDS {
        println!("==== ROUND {:04} ====", round_number);
        println!("{} messages pending...", network.len());

        // With low probability, drop a message
        if !network.is_empty() && rng.random_bool(LOSS_PROBABILITY) {
            let msg = network.next_msg();
            if let Some(msg) = msg {
                println!("Dropping message {:?}", msg);
            }
        }
        // With slightly higher probability, delay a message
        if network.len() >= 2 && rng.random_bool(DELAY_PROBABILITY) {
            network.delay();
            println!("Delaying a message");
        }

        // If there's nothing in the queue, or with some probability, take a random process
        // and cause it to issue a proposal. Processes that have decided a value are exempt.
        let msgs = if network.is_empty() || rng.random_bool(0.05) {
            let random_undecided_process = processes
                .iter_mut()
                .filter(|p| p.decided_value.is_none())
                .choose(&mut rng);

            match random_undecided_process {
                Some(p) => {
                    println!("Random proposal from {}!", p.id.0);
                    p.create_proposal_messages()
                }
                None => {
                    println!("Everyone has decided on a value!");
                    break;
                }
            }
        } else {
            let msg = network.next_msg().unwrap();
            println!(
                "Applying a message: {} -> {}: {:?}",
                msg.from.0, msg.to.0, msg.msg
            );
            processes[msg.to.0].recv_message(msg)
        };

        println!("Sending {} messages:", msgs.len());
        for msg in &msgs {
            println!("  {} to {}: {:?}", msg.from.0, msg.to.0, msg.msg);
        }

        network.enqueue(msgs);

        // Print current status
        for p in &processes {
            println!("{}: {}", p.id.0, p.status());
        }
        println!("====================");
    }

    println!("======== END OF SIMULATION ========");
    for p in &processes {
        println!(
            "Process {} has decided on value {}",
            p.id.0,
            p.decided_value.as_ref().map_or("NONE", |s| s.as_str())
        )
    }
    if processes
        .iter()
        .all(|p| p.decided_value == processes[0].decided_value)
    {
        println!("SUCCESS!");
    } else {
        println!("FAILURE!");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ProcessID(usize);

#[derive(Debug)]
struct Process {
    id: ProcessID,
    // proposer
    current_proposal_id: Option<usize>,
    promises_received: HashMap<ProcessID, Option<(ProposalID, String)>>,
    // acceptor
    latest_promised: Option<ProposalID>,
    latest_accepted: Option<(ProposalID, String)>,
    // learner
    acceptances_received: HashMap<ProposalID, (usize, String)>,
    decided_value: Option<String>,
}

#[derive(Debug)]
struct AddressedMessage {
    from: ProcessID,
    to: ProcessID,
    msg: Message,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ProposalID(usize, ProcessID);

#[derive(Debug, Clone)]
enum Message {
    Prepare(usize),
    Promise(usize, Option<(ProposalID, String)>),
    Accept(usize, String),
    Accepted(ProposalID, String), // is value needed?
}

impl Process {
    fn new(id: ProcessID) -> Self {
        Self {
            id,
            current_proposal_id: None,
            promises_received: HashMap::new(),
            latest_promised: None,
            latest_accepted: None,
            acceptances_received: HashMap::new(),
            decided_value: None,
        }
    }

    fn msg_everybody(&self, msg: Message) -> Vec<AddressedMessage> {
        (0..N)
            .map(|i| AddressedMessage {
                from: self.id,
                to: ProcessID(i),
                msg: msg.clone(),
            })
            .collect()
    }

    fn create_proposal_messages(&mut self) -> Vec<AddressedMessage> {
        // Increment and fetch
        let n = self.current_proposal_id.map_or(0, |x| x + 1);
        self.current_proposal_id = Some(n);

        // Wipe all knowledge of previous proposals
        self.promises_received.clear();

        self.msg_everybody(Message::Prepare(n))
    }

    fn recv_message(&mut self, msg: AddressedMessage) -> Vec<AddressedMessage> {
        match msg.msg {
            Message::Prepare(n) => {
                let proposal = ProposalID(n, msg.from);
                if self.latest_promised.is_none_or(|old| proposal > old) {
                    // make a promise, but do not accept a value (you haven't gotten one
                    // for this proposal yet!)
                    self.latest_promised = Some(proposal);
                    vec![AddressedMessage {
                        from: self.id,
                        to: msg.from,
                        msg: Message::Promise(n, self.latest_accepted.clone()),
                    }]
                } else {
                    // ignore it if we have already made a later promis than this one
                    // TODO: impl NAK
                    vec![]
                }
            }
            Message::Promise(n, latest_accepted) => {
                if Some(n) != self.current_proposal_id {
                    // ignore this, it's from some other older proposal of ours
                    return vec![];
                }

                // if we already had quorum, don't even bother, we've already sent
                // acceptances for this proposal
                if self.promises_received.len() > F {
                    return vec![];
                }

                // add it and see if we have quorum
                self.promises_received.insert(msg.from, latest_accepted);
                if self.promises_received.len() > F {
                    // take the most recently accepted value, or make our own
                    let value = self
                        .promises_received
                        .values()
                        .flatten()
                        .max_by_key(|(proposal, _)| proposal)
                        .map_or(format!("rand({:?}, {})", self.id, n), |(_, value)| {
                            value.clone()
                        });

                    // now send that message out to a quorum
                    self.msg_everybody(Message::Accept(n, value))
                } else {
                    // no quorum yet
                    vec![]
                }
            }
            // We're an Acceptor and we got an Accept message! We should accept it unless
            // we've made a higher-numbered Promise.
            Message::Accept(n, value) => {
                let proposal_id = ProposalID(n, msg.from);
                if self.latest_promised.is_none_or(|old| proposal_id >= old) {
                    // accept it!
                    self.latest_accepted = Some((proposal_id, value.clone()));
                    self.msg_everybody(Message::Accepted(proposal_id, value))
                } else {
                    // ignore (or NAK)
                    vec![]
                }
            }
            // We're a Proposer and our value was accepted! Count how many Accepteds we get,
            // until we get a majority.
            Message::Accepted(proposal_id, value) => {
                match self.acceptances_received.get_mut(&proposal_id) {
                    Some((n, v)) => {
                        assert_eq!(value, *v, "{:?}", proposal_id);
                        *n += 1;
                        if *n > F {
                            // great, it's decided!
                            self.decided_value = Some(value)
                        }
                    }
                    None => {
                        self.acceptances_received.insert(proposal_id, (0, value));
                    }
                }
                // never say anything
                vec![]
            }
        }
    }

    fn status(&self) -> String {
        format!("{:?}", self)
    }
}

#[derive(Debug)]
struct Network {
    in_flight: VecDeque<AddressedMessage>,
}

impl Network {
    fn new() -> Self {
        Self {
            in_flight: VecDeque::new(),
        }
    }

    fn enqueue(&mut self, msgs: Vec<AddressedMessage>) {
        self.in_flight.extend(msgs);
    }

    fn next_msg(&mut self) -> Option<AddressedMessage> {
        self.in_flight.pop_front()
    }

    fn is_empty(&self) -> bool {
        self.in_flight.is_empty()
    }

    fn len(&self) -> usize {
        self.in_flight.len()
    }

    /// swaps the first and second elements in the queue
    fn delay(&mut self) {
        if self.in_flight.len() >= 2 {
            self.in_flight.swap(0, 1);
        }
    }
}
