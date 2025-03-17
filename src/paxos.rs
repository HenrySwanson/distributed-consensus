use std::collections::HashMap;
use std::fmt::Display;

use itertools::Itertools;
use rand::Rng;

use crate::simulation::Context;
use crate::simulation::Incoming;
use crate::simulation::Outgoing;
use crate::simulation::ProcessID;
use crate::F;
use crate::N;

const ENABLE_NACKS: bool = true;
const PROPOSAL_COOLDOWN: u64 = 10;
const PROPOSAL_PROBABILITY: f64 = 0.05;

#[derive(Debug)]
pub struct Process {
    pub id: ProcessID,
    // proposer
    current_proposal_id: Option<usize>,
    promises_received: HashMap<ProcessID, Option<(ProposalID, String)>>,
    superseded_by: Option<ProposalID>,
    min_next_proposal_time: u64,
    // acceptor
    latest_promised: Option<ProposalID>,
    latest_accepted: Option<(ProposalID, String)>,
    // learner
    acceptances_received: HashMap<ProposalID, (usize, String)>,
    decided_value: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProposalID(usize, ProcessID);

#[derive(Debug, Clone)]
pub enum Message {
    Prepare(usize),
    Promise(usize, Option<(ProposalID, String)>),
    Accept(usize, String),
    Accepted(ProposalID, String), // is value needed?
    Nack(ProposalID),
}

impl Process {
    pub fn new(id: ProcessID) -> Self {
        Self {
            id,
            current_proposal_id: None,
            promises_received: HashMap::new(),
            superseded_by: None,
            min_next_proposal_time: 0,
            latest_promised: None,
            latest_accepted: None,
            acceptances_received: HashMap::new(),
            decided_value: None,
        }
    }

    pub fn tick(&mut self, ctx: Context) {
        // First check the timer and maybe fire a proposal message
        if self.decided_value.is_none()
            && self.min_next_proposal_time <= ctx.current_tick
            && ctx.rng.random_bool(PROPOSAL_PROBABILITY)
        {
            log::trace!("Random proposal from {}!", self.id.0);
            let proposal_msgs = self.create_proposal_messages(ctx.current_tick);
            ctx.outgoing_messages.extend(proposal_msgs);
        }

        // Then process all messages received
        for msg in ctx.received_messages {
            ctx.outgoing_messages
                .extend(self.recv_message(msg, ctx.current_tick))
        }
    }

    pub fn crash(&mut self) {
        // replace self with a fresh process, only carrying over a little info
        let old = std::mem::replace(self, Self::new(self.id));
        self.latest_promised = old.latest_promised;
        self.latest_accepted = old.latest_accepted;
    }

    fn msg_everybody(&self, msg: Message) -> Vec<Outgoing<Message>> {
        (0..N)
            .map(|i| Outgoing {
                to: ProcessID(i),
                msg: msg.clone(),
            })
            .collect()
    }

    fn create_proposal_messages(&mut self, current_tick: u64) -> Vec<Outgoing<Message>> {
        // Send something higher than:
        // - our previous proposal
        // - anything we've ever seen from a Nack
        let latest = std::cmp::max(self.current_proposal_id, self.superseded_by.map(|p| p.0));
        let n = latest.map_or(0, |x| x + 1);
        self.current_proposal_id = Some(n);

        // Wipe all knowledge of previous proposals
        self.promises_received.clear();
        self.superseded_by = None;

        // Set the proposal cooldown timer
        self.min_next_proposal_time = current_tick + PROPOSAL_COOLDOWN;

        self.msg_everybody(Message::Prepare(n))
    }

    fn recv_message(
        &mut self,
        msg: Incoming<Message>,
        current_tick: u64,
    ) -> Vec<Outgoing<Message>> {
        // there's network activity, cool down the proposal timer
        self.min_next_proposal_time = current_tick + PROPOSAL_COOLDOWN;
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
                vec![Outgoing {
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
                if let Some(n) = self.current_proposal_id {
                    let current_proposal = ProposalID(n, self.id);
                    if proposal > current_proposal {
                        self.superseded_by = Some(proposal);
                    }
                } else {
                    // uncommon, but it can happen if we've crashed and forgotten
                    // our own proposals. do nothing in this case
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
    pub fn status(&self) -> String {
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

    pub fn decided_value(&self) -> Option<&String> {
        self.decided_value.as_ref()
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

impl Display for ProposalID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "P{}.{}", self.0, self.1)
    }
}
