use std::collections::HashMap;
use std::collections::HashSet;

use itertools::Itertools;
use rand::Rng;

use super::get_mut_extending_if_necessary;
use super::ProposalID;
use super::ENABLE_NACKS;
use super::MAX_LOG_SIZE;
use super::PROPOSAL_COOLDOWN;
use super::PROPOSAL_PROBABILITY;
use super::TARGET_LOG_SIZE;
use crate::simulation::Incoming;
use crate::simulation::Outgoing;
use crate::simulation::Process;
use crate::simulation::ProcessID;
use crate::F;
use crate::N;

const HEARTBEAT_INTERVAL: u64 = PROPOSAL_COOLDOWN / 2;
const NEXT_MSG_INTERVAL: u64 = 20;

#[derive(Debug)]
pub struct MultiPaxos {
    id: ProcessID,
    phase: Phase,
    // gotta track this even when we're not a leader
    last_issued_proposal: Option<usize>,
    latest_promised: Option<ProposalID>,
    log: Vec<LogEntry>,
}

#[derive(Debug)]
enum Phase {
    // TODO: split into phase 1 and phase 2?
    Leader {
        first_unchosen: usize,
        promises_received: HashMap<ProcessID, PreviouslyAccepted>,
        uncommitted_slots: HashMap<usize, (String, HashSet<ProcessID>)>,
        next_heartbeat_time: u64,
        min_next_msg_time: u64,
        value_counter: u64, // for generating different values
    },
    Follower {
        min_next_proposal_time: u64,
    },
}

#[derive(Debug, Clone)]
pub enum Message {
    Prepare(usize, usize),
    Promise(usize, PreviouslyAccepted),
    Accept(usize, usize, String),
    Accepted(usize, usize),
    Learned(usize, usize, String),
    Nack(ProposalID),
    Heartbeat,
}

// TODO: also return previously committed
type PreviouslyAccepted = HashMap<usize, (ProposalID, String)>;

#[derive(Debug)]
enum LogEntry {
    Empty,
    Accepted(ProposalID, String),
    Committed(ProposalID, String), // TODO: drop proposal id
}

impl Process for MultiPaxos {
    type Message = Message;
    type Consensus = Vec<Option<String>>;

    fn new(id: ProcessID) -> Self {
        Self {
            id,
            last_issued_proposal: None,
            latest_promised: None,
            log: vec![],
            phase: Phase::Follower {
                min_next_proposal_time: PROPOSAL_COOLDOWN,
            },
        }
    }

    fn tick(&mut self, ctx: crate::simulation::Context<Self::Message>) {
        // First, process all messages received
        for msg in ctx.received_messages {
            ctx.outgoing_messages
                .extend(self.recv_message(msg, ctx.current_tick));
        }

        // Then, check the timeout
        match &mut self.phase {
            Phase::Leader {
                first_unchosen: _,
                promises_received,
                uncommitted_slots,
                next_heartbeat_time,
                min_next_msg_time,
                value_counter,
            } => {
                if *next_heartbeat_time <= ctx.current_tick {
                    ctx.outgoing_messages
                        .extend(msg_everybody_except(Message::Heartbeat, self.id));
                    *next_heartbeat_time = ctx.current_tick + HEARTBEAT_INTERVAL;
                }
                if *min_next_msg_time <= ctx.current_tick
                    && promises_received.len() > F
                    // ^^can only send new messages after phase 1!
                    // this is messy... can we do better?
                    && self.log.len() < MAX_LOG_SIZE
                    && ctx.rng.random_bool(PROPOSAL_PROBABILITY)
                {
                    log::trace!("Random command from {}!", self.id.0);

                    let n = self
                        .last_issued_proposal
                        .expect("need active proposal when leader");
                    let slot = self.log.len();
                    let value = format!("V{}.{}.{}", n, self.id, value_counter);
                    *value_counter += 1;

                    ctx.outgoing_messages.extend(msg_everybody_except(
                        Message::Accept(n, self.log.len(), value.clone()),
                        self.id,
                    ));

                    // fake an accept to self
                    self.log
                        .push(LogEntry::Accepted(ProposalID(n, self.id), value.clone()));

                    // fake an accepted from self
                    uncommitted_slots.insert(slot, (value, HashSet::from([self.id])));
                }
            }
            Phase::Follower {
                min_next_proposal_time,
                ..
            } => {
                if *min_next_proposal_time <= ctx.current_tick
                    && ctx.rng.random_bool(PROPOSAL_PROBABILITY)
                {
                    // convert to leader and make a proposal
                    log::trace!("Random proposal from {}!", self.id.0);
                    let proposal_msg = self.start_proposal(ctx.current_tick);

                    ctx.outgoing_messages
                        .extend(self.msg_everybody_else(proposal_msg))
                }
            }
        }
    }

    fn crash(&mut self) {
        // replace self with a fresh process, only carrying over a little info
        let old = std::mem::replace(self, Self::new(self.id));
        self.last_issued_proposal = old.last_issued_proposal;
        self.latest_promised = old.latest_promised;
        self.log = old.log;
    }

    fn status(&self) -> String {
        // TODO: make this much more detailed
        format!(
            "Process #{}: ({}) Decided values: {}",
            self.id,
            match &self.phase {
                Phase::Leader {
                    promises_received, ..
                } =>
                    if promises_received.len() > F {
                        "Leader+"
                    } else {
                        "Leader"
                    },
                Phase::Follower { .. } => "Follower",
            },
            self.log
                .iter()
                .map(|entry| match entry {
                    LogEntry::Empty => format!("EMPTY"),
                    LogEntry::Accepted(_, v) => format!("{v}*"),
                    LogEntry::Committed(_, v) => format!("{}", v),
                })
                .format(",")
        )
    }

    fn is_done(&self) -> bool {
        self.decided_value()
            .into_iter()
            .chain(std::iter::repeat(None))
            .take(TARGET_LOG_SIZE)
            .all(|x| x.is_some())
    }

    fn decided_value(&self) -> Self::Consensus {
        self.log
            .iter()
            .map(|entry| match entry {
                LogEntry::Empty => None,
                LogEntry::Accepted(_, _) => None,
                LogEntry::Committed(_, v) => Some(v.clone()),
            })
            .collect()
    }
}

impl MultiPaxos {
    fn msg_everybody_else(&self, msg: Message) -> Vec<Outgoing<Message>> {
        msg_everybody_except(msg, self.id)
    }

    fn start_proposal(&mut self, current_tick: u64) -> Message {
        // our proposal should be higher than:
        // - anything we've sent
        // - anything we've ever seen
        let latest = std::cmp::max(self.last_issued_proposal, self.latest_promised.map(|p| p.0));
        let n = latest.map_or(0, |x| x + 1);
        self.last_issued_proposal = Some(n);

        // find the index of our first uncommitted entry
        let first_unchosen = self
            .log
            .iter()
            .take_while(|entry| matches!(entry, LogEntry::Committed(_, _)))
            .count();

        // simulate receiving a prepare from yourself
        let promise_from_self = self.get_previously_accepted(first_unchosen);
        self.latest_promised = Some(ProposalID(n, self.id));

        // set as leader
        self.phase = Phase::Leader {
            first_unchosen,
            promises_received: HashMap::from([(self.id, promise_from_self)]),
            next_heartbeat_time: current_tick + HEARTBEAT_INTERVAL,
            uncommitted_slots: HashMap::new(),
            min_next_msg_time: current_tick + NEXT_MSG_INTERVAL,
            value_counter: 0,
        };

        Message::Prepare(n, first_unchosen)
    }

    fn recv_message(
        &mut self,
        msg: Incoming<Message>,
        current_tick: u64,
    ) -> Vec<Outgoing<Message>> {
        // what are we doing?
        match &mut self.phase {
            Phase::Leader {
                first_unchosen,
                promises_received,
                uncommitted_slots,
                ..
            } => {
                let current_n = self
                    .last_issued_proposal
                    .expect("leader must have active proposal");
                let current_proposal_id = ProposalID(current_n, self.id);
                match msg.msg {
                    // messages we expect
                    Message::Promise(n, previously_accepted) => {
                        // check that it's from our current proposal
                        assert!(n <= current_n);
                        if n != current_n {
                            return vec![];
                        }

                        // if we already had quorum, don't re-send the accepts
                        if promises_received.len() > F {
                            return vec![];
                        }

                        // add this response and see if we have quorum
                        promises_received.insert(msg.from, previously_accepted);
                        if promises_received.len() <= F {
                            // no quorum yet
                            return vec![];
                        }

                        log::trace!("Reached quorum for proposal {}", current_proposal_id);

                        // figure out what values we should propose
                        let mut values = promises_received
                            .clone()
                            .into_values()
                            .flatten()
                            .into_grouping_map()
                            .max_by_key(|_slot, (proposal_id, _val)| proposal_id.clone());

                        // now fill all our gaps until we've used up all the values we
                        // need to propagate
                        assert_eq!(*uncommitted_slots, HashMap::new());
                        let mut accept_msgs = vec![];
                        for slot in *first_unchosen.. {
                            if values.is_empty() {
                                break;
                            }

                            let value = match values.remove(&slot) {
                                Some((_old_proposal_id, value)) => value,
                                None => String::from("NO-OP"),
                            };

                            accept_msgs.extend(msg_everybody_except(
                                Message::Accept(
                                    self.last_issued_proposal
                                        .expect("need active proposal when leader"),
                                    slot,
                                    value.clone(),
                                ),
                                self.id,
                            ));

                            // fake an accept to self
                            let entry = get_mut_extending_if_necessary(&mut self.log, slot, || {
                                LogEntry::Empty
                            });
                            *entry = LogEntry::Accepted(current_proposal_id, value.clone());

                            // fake an accepted from self
                            uncommitted_slots.insert(slot, (value, HashSet::from([self.id])));
                        }

                        accept_msgs
                    }
                    Message::Accepted(n, slot) => {
                        // check that it's from our current proposal
                        assert!(n <= current_n);
                        if n != current_n {
                            return vec![];
                        }

                        // this should always be fine because we checked the proposal id above
                        let (value, acceptors) = uncommitted_slots
                            .get_mut(&slot)
                            .expect("Accepted can only happen when we initiate it");
                        let value = value.clone();

                        // do we already have quorum? if so, ignore this
                        if acceptors.len() > F {
                            return vec![];
                        }

                        // add this accept and see if we have quorum
                        acceptors.insert(msg.from);
                        if acceptors.len() <= F {
                            // no quorum yet
                            return vec![];
                        }

                        // great! we can commit this one
                        *self
                            .log
                            .get_mut(slot)
                            .expect("self-accept should've initialized this one") =
                            LogEntry::Committed(current_proposal_id, value.clone());

                        self.msg_everybody_else(Message::Learned(current_n, slot, value))
                    }
                    Message::Nack(proposal_id) => {
                        // did we get pre-empted?
                        if proposal_id > current_proposal_id {
                            // switch to follower!
                            self.phase = Phase::Follower {
                                min_next_proposal_time: current_tick + PROPOSAL_COOLDOWN,
                            };
                        }

                        vec![]
                    }

                    // messages that might knock us out of leadership
                    Message::Prepare(n, _)
                    | Message::Accept(n, _, _)
                    | Message::Learned(n, _, _) => {
                        let other_proposal_id = ProposalID(n, msg.from);
                        if other_proposal_id > current_proposal_id {
                            // switch to follower and reprocess message
                            self.phase = Phase::Follower {
                                min_next_proposal_time: current_tick + PROPOSAL_COOLDOWN,
                            };
                            self.recv_message(msg, current_tick)
                        } else {
                            // otherwise it's fine, just ignore it
                            vec![]
                        }
                    }
                    Message::Heartbeat => {
                        // some other leader is out there, or we're getting
                        // old heartbeats from an old leader. either way, ignore.
                        vec![]
                    }
                }
            }
            Phase::Follower {
                min_next_proposal_time,
            } => {
                // cool down the proposal timer
                // TODO: should this only be for certain messages?
                *min_next_proposal_time = current_tick + PROPOSAL_COOLDOWN;
                match msg.msg {
                    // messages we expect
                    Message::Prepare(n, first_unchosen) => {
                        let proposal_id = ProposalID(n, msg.from);
                        // check if this is newer than our current proposal
                        let reply = if self
                            .latest_promised
                            .is_none_or(|latest_promised| proposal_id >= latest_promised)
                        {
                            self.latest_promised = Some(proposal_id);

                            // reply with all the entries leader requested
                            Message::Promise(n, self.get_previously_accepted(first_unchosen))
                        } else {
                            // NACK it
                            if ENABLE_NACKS {
                                Message::Nack(self.latest_promised.unwrap())
                            } else {
                                return vec![];
                            }
                        };

                        // TODO: reply() method
                        vec![Outgoing {
                            to: msg.from,
                            msg: reply,
                        }]
                    }
                    Message::Accept(n, slot, value) => {
                        let proposal_id = ProposalID(n, msg.from);
                        // accept it unless we've made a higher-numbered promise
                        let reply = if self
                            .latest_promised
                            .is_none_or(|latest_promised| proposal_id >= latest_promised)
                        {
                            self.latest_promised = Some(proposal_id);
                            let entry = get_mut_extending_if_necessary(&mut self.log, slot, || {
                                LogEntry::Empty
                            });
                            // sanity check
                            match entry {
                                LogEntry::Empty => {}
                                LogEntry::Accepted(old_proposal_id, _) => {
                                    assert!(
                                        proposal_id >= *old_proposal_id,
                                        "{} < {}",
                                        proposal_id,
                                        old_proposal_id
                                    );
                                }
                                LogEntry::Committed(old_proposal_id, chosen_value) => {
                                    assert!(proposal_id >= *old_proposal_id);
                                    assert_eq!(value, *chosen_value);
                                }
                            }
                            *entry = LogEntry::Accepted(proposal_id, value);

                            // tell the leader we accepted
                            Message::Accepted(n, slot)
                        } else {
                            // ignore (TODO: NACK)
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
                    Message::Learned(n, slot, value) => {
                        // unconditional accept
                        let proposal_id = ProposalID(n, msg.from);
                        let entry =
                            get_mut_extending_if_necessary(&mut self.log, slot, || LogEntry::Empty);
                        // sanity check
                        match entry {
                            LogEntry::Empty => {}
                            LogEntry::Accepted(_, _) => {}
                            LogEntry::Committed(_, chosen_value) => {
                                assert_eq!(value, *chosen_value);
                            }
                        }
                        *entry = LogEntry::Committed(proposal_id, value);
                        vec![]
                    }
                    // we are no longer a leader, we can't do anything about these
                    Message::Promise(_, _) | Message::Accepted(_, _) | Message::Nack(_) => {
                        vec![]
                    }

                    // we already handle the timer up above, this message doesn't need
                    // any further processing
                    Message::Heartbeat => {
                        vec![]
                    }
                }
            }
        }
    }

    fn get_previously_accepted(&self, first_unchosen: usize) -> PreviouslyAccepted {
        // WARNING: infinite iterator! `map_while` makes it finite
        (first_unchosen..)
            .map_while(|slot| {
                let entry = self.log.get(slot)?;
                match entry {
                    LogEntry::Empty => None,
                    LogEntry::Accepted(proposal_id, value)
                    | LogEntry::Committed(proposal_id, value) => {
                        Some((slot, (*proposal_id, value.clone())))
                    }
                }
            })
            .collect()
    }
}

// TODO: remove this somehow
fn msg_everybody_except(msg: Message, id: ProcessID) -> Vec<Outgoing<Message>> {
    (0..N)
        .filter_map(|i| {
            if i == id.0 {
                None
            } else {
                Some(Outgoing {
                    to: ProcessID(i),
                    msg: msg.clone(),
                })
            }
        })
        .collect()
}
