use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::Display;

use itertools::Itertools;
use rand::seq::IteratorRandom;
use rand::Rng;

const F: usize = 2;
const N: usize = 2 * F + 1;

const MAX_ROUNDS: usize = 10000;
const LOSS_PROBABILITY: f64 = 0.1;
const DELAY_PROBABILITY: f64 = 0.2;
const ENABLE_NACKS: bool = true;

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
            network.drop();
        }
        // With slightly higher probability, delay a message
        if network.len() >= 2 && rng.random_bool(DELAY_PROBABILITY) {
            network.delay();
        }

        // If there's nothing in the queue, or with some probability, take a random process
        // and cause it to issue a proposal. Processes that have decided a value are exempt.
        if network.is_empty() || rng.random_bool(0.05) {
            let random_undecided_process = processes
                .iter_mut()
                .filter(|p| p.decided_value.is_none())
                .choose(&mut rng);

            match random_undecided_process {
                Some(p) => {
                    println!("Random proposal from {}!", p.id.0);
                    let proposal_msgs = p.create_proposal_messages();
                    network.enqueue(proposal_msgs);
                }
                None => {
                    println!("Everyone has decided on a value!");
                    break;
                }
            }
        } else {
            let msg = network.next_msg().unwrap();
            let replies = processes[msg.to.0].recv_message(msg);
            network.enqueue(replies);
        };

        // Print current status
        for p in &processes {
            println!("{}", p.status());
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
    superseded_by: Option<ProposalID>,
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
    Nack(ProposalID),
}

impl Process {
    fn new(id: ProcessID) -> Self {
        Self {
            id,
            current_proposal_id: None,
            promises_received: HashMap::new(),
            superseded_by: None,
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
        // Send something higher than:
        // - our previous proposal
        // - anything we've ever seen from a Nack
        let latest = std::cmp::max(self.current_proposal_id, self.superseded_by.map(|p| p.0));
        let n = latest.map_or(0, |x| x + 1);
        self.current_proposal_id = Some(n);

        // Wipe all knowledge of previous proposals
        self.promises_received.clear();
        self.superseded_by = None;

        self.msg_everybody(Message::Prepare(n))
    }

    fn recv_message(&mut self, msg: AddressedMessage) -> Vec<AddressedMessage> {
        match msg.msg {
            Message::Prepare(n) => {
                let proposal = ProposalID(n, msg.from);
                let reply = if self.latest_promised.is_none_or(|old| proposal > old) {
                    // make a promise, but do not accept a value (you haven't gotten one
                    // for this proposal yet!)
                    self.latest_promised = Some(proposal);
                    Message::Promise(n, self.latest_accepted.clone())
                } else {
                    // NACK it if we have already made a later promise than this one
                    if ENABLE_NACKS {
                        Message::Nack(self.latest_promised.unwrap())
                    } else {
                        return vec![];
                    }
                };
                vec![AddressedMessage {
                    from: self.id,
                    to: msg.from,
                    msg: reply,
                }]
            }
            Message::Promise(n, latest_accepted) => {
                if Some(n) != self.current_proposal_id {
                    // ignore this, it's from some other older proposal of ours
                    return vec![];
                }

                // if our current proposal got NACKed, ignore this promise
                if self.superseded_by.is_some() {
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
                        .map_or(format!("V{}.{}", n, self.id), |(_, value)| value.clone());

                    // now send that message out to a quorum
                    self.msg_everybody(Message::Accept(n, value))
                } else {
                    // no quorum yet
                    vec![]
                }
            }
            // We're a Proposer, but we've been told by one of the Acceptors we're talking
            // to that they have already promised to a higher proposal.
            Message::Nack(proposal) => {
                // we got a NACK, which should indicate that we abort the proposal
                // we're doing. but it could be stale, so ignore it if so
                let current_proposal = ProposalID(
                    self.current_proposal_id
                        .expect("got nack before sending any proposal"),
                    self.id,
                );

                assert_ne!(proposal, current_proposal);
                if proposal > current_proposal {
                    self.superseded_by = Some(proposal);
                }

                // in all cases, don't respond
                vec![]
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

    // TODO: column-based? idk
    fn status(&self) -> String {
        format!(
            "Process #{}: P {{ {}, [{}], superseded by: {} }}, A {{ {}, {} }}, L {{ {}, {} }}",
            self.id,
            // proposer
            display_or_none(&self.current_proposal_id),
            self.promises_received
                .iter()
                .map(|(id, last_accepted)| format!("{} {}", id, display_or_none2(last_accepted),))
                .format(", "),
            display_or_none(&self.superseded_by),
            // acceptor
            display_or_none(&self.latest_promised),
            display_or_none2(&self.latest_accepted),
            // learner
            format_args!("hashmap of size {}", self.acceptances_received.len()),
            display_or_none(&self.decided_value)
        )
    }
}

fn display_or_none<T: Display>(t: &Option<T>) -> String {
    match t {
        Some(t) => t.to_string(),
        None => String::from("None"),
    }
}

fn display_or_none2<T: Display, U: Display>(t: &Option<(T, U)>) -> String {
    match t {
        Some((t, u)) => format!("({t}, {u})"),
        None => String::from("None"),
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
        println!("Sending {} messages:", msgs.len());
        for msg in &msgs {
            println!("  {} to {}: {:?}", msg.from.0, msg.to.0, msg.msg);
        }
        self.in_flight.extend(msgs);
    }

    fn next_msg(&mut self) -> Option<AddressedMessage> {
        let msg = self.in_flight.pop_front()?;
        println!(
            "Received a message:  {} -> {}: {:?}",
            msg.from.0, msg.to.0, msg.msg
        );
        Some(msg)
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
            println!("Delaying message: {:?}", self.in_flight[0]);
            self.in_flight.swap(0, 1);
        }
    }

    /// drops the first message
    fn drop(&mut self) {
        if let Some(msg) = self.in_flight.pop_front() {
            println!("Dropping message {:?}", msg);
        }
    }
}

impl Display for ProcessID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Display for ProposalID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "P{}.{}", self.0, self.1)
    }
}
