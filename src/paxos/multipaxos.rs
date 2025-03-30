use std::collections::HashMap;
use std::collections::HashSet;

use all_asserts::assert_ge;
use all_asserts::assert_le;
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
use crate::N;
use crate::QUORUM;

const HEARTBEAT_INTERVAL: u64 = PROPOSAL_COOLDOWN / 2;
const NEXT_MSG_INTERVAL: u64 = 20;

#[derive(Debug)]
pub struct MultiPaxos {
    common: Common,
    phase: Phase,
}

#[derive(Debug)]
struct Common {
    id: ProcessID,
    last_issued_proposal: Option<usize>,
    latest_promised: Option<ProposalID>,
    log: Log,
}

#[derive(Debug)]
enum Phase {
    Leader(Leader),
    Follower(Follower),
}

#[derive(Debug)]
// TODO: split into phase 1 and phase 2?
struct Leader {
    unchosen_gaps: Gaps,
    promises_received: HashMap<ProcessID, PreviouslyAccepted>,
    uncommitted_slots: HashMap<usize, (String, HashSet<ProcessID>)>,
    next_heartbeat_time: u64,
    min_next_msg_time: u64,
    value_counter: u64, // for generating different values
}

#[derive(Debug)]
struct Follower {
    min_next_proposal_time: u64,
}

#[derive(Debug, Clone)]
pub enum Message {
    Prepare(usize, Gaps),
    Promise(usize, PreviouslyAccepted),
    Accept(usize, usize, String),
    Accepted(usize, usize),
    Learned(usize, usize, String), // TODO: don't need proposal number?
    Nack(ProposalID),
    Heartbeat,
}

#[derive(Debug, Clone)]
pub struct Gaps {
    interior: Vec<usize>,
    tail_start: usize,
}

// None for proposal id indicates committed
// TODO: make this better structured
type PreviouslyAccepted = HashMap<usize, (Option<ProposalID>, String)>;

#[derive(Debug)]
struct Log(Vec<LogEntry>);

#[derive(Debug)]
enum LogEntry {
    Empty,
    Accepted(ProposalID, String),
    Committed(String),
}

impl Process for MultiPaxos {
    type Message = Message;
    type Consensus = Vec<Option<String>>;

    fn new(id: ProcessID) -> Self {
        Self {
            common: Common {
                id,
                last_issued_proposal: None,
                latest_promised: None,
                log: Log::new(),
            },
            phase: Phase::Follower(Follower {
                min_next_proposal_time: PROPOSAL_COOLDOWN,
            }),
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
            Phase::Leader(leader) => {
                if leader.next_heartbeat_time <= ctx.current_tick {
                    ctx.outgoing_messages
                        .extend(msg_everybody_except(Message::Heartbeat, self.common.id));
                    leader.next_heartbeat_time = ctx.current_tick + HEARTBEAT_INTERVAL;
                }
                if leader.min_next_msg_time <= ctx.current_tick
                    && leader.promises_received.len() >= QUORUM
                    // ^^can only send new messages after phase 1!
                    // this is messy... can we do better?
                    && self.common.log.len() < MAX_LOG_SIZE
                    && ctx.rng.random_bool(PROPOSAL_PROBABILITY)
                {
                    log::trace!("Random command from {}!", self.common.id.0);

                    let n = self
                        .common
                        .last_issued_proposal
                        .expect("need active proposal when leader");
                    let slot = self.common.log.len();
                    let value = format!("V{}.{}.{}", n, self.common.id, leader.value_counter);
                    leader.value_counter += 1;

                    let proposal_id = ProposalID(n, self.common.id);
                    let accept_msgs = leader.start_accept_phase(
                        &mut self.common,
                        proposal_id,
                        slot,
                        value.clone(),
                    );

                    ctx.outgoing_messages.extend(accept_msgs);
                }
            }
            Phase::Follower(follower) => {
                if follower.min_next_proposal_time <= ctx.current_tick
                    && ctx.rng.random_bool(PROPOSAL_PROBABILITY)
                {
                    // convert to leader and make a proposal
                    log::trace!("Random proposal from {}!", self.common.id.0);
                    let proposal_msg = self.start_proposal(ctx.current_tick);

                    ctx.outgoing_messages
                        .extend(self.msg_everybody_else(proposal_msg))
                }
            }
        }
    }

    fn crash(&mut self) {
        // replace self with a fresh process, only carrying over a little info
        let old = std::mem::replace(self, Self::new(self.common.id));
        self.common.last_issued_proposal = old.common.last_issued_proposal;
        self.common.latest_promised = old.common.latest_promised;
        self.common.log = old.common.log;
    }

    fn status(&self) -> String {
        // TODO: make this much more detailed
        format!(
            "Process #{}: ({}) Log: {}",
            self.common.id,
            match &self.phase {
                Phase::Leader(leader) =>
                    if leader.promises_received.len() >= QUORUM {
                        "Leader+"
                    } else {
                        "Leader"
                    },
                Phase::Follower(_) => "Follower",
            },
            self.common
                .log
                .iter()
                .map(|entry| match entry {
                    LogEntry::Empty => "EMPTY".to_string(),
                    LogEntry::Accepted(_, v) => format!("{v}*"),
                    LogEntry::Committed(v) => v.to_string(),
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
        self.common
            .log
            .iter()
            .map(|entry| match entry {
                LogEntry::Empty => None,
                LogEntry::Accepted(_, _) => None,
                LogEntry::Committed(v) => Some(v.clone()),
            })
            .collect()
    }
}

impl MultiPaxos {
    fn msg_everybody_else(&self, msg: Message) -> Vec<Outgoing<Message>> {
        msg_everybody_except(msg, self.common.id)
    }

    fn start_proposal(&mut self, current_tick: u64) -> Message {
        // our proposal should be higher than:
        // - anything we've sent
        // - anything we've ever seen
        let latest = std::cmp::max(
            self.common.last_issued_proposal,
            self.common.latest_promised.map(|p| p.0),
        );
        let n = latest.map_or(0, |x| x + 1);
        self.common.last_issued_proposal = Some(n);

        // find all uncommitted entries
        let unchosen_gaps = self.common.find_gaps();

        // simulate receiving a prepare from yourself
        let promise_from_self = self.get_previously_accepted(&unchosen_gaps);
        self.common.latest_promised = Some(ProposalID(n, self.common.id));

        // set as leader
        self.phase = Phase::Leader(Leader {
            unchosen_gaps: unchosen_gaps.clone(),
            promises_received: HashMap::from([(self.common.id, promise_from_self)]),
            next_heartbeat_time: current_tick + HEARTBEAT_INTERVAL,
            uncommitted_slots: HashMap::new(),
            min_next_msg_time: current_tick + NEXT_MSG_INTERVAL,
            value_counter: 0,
        });

        Message::Prepare(n, unchosen_gaps)
    }

    fn recv_message(
        &mut self,
        msg: Incoming<Message>,
        current_tick: u64,
    ) -> Vec<Outgoing<Message>> {
        // what are we doing?
        match &mut self.phase {
            Phase::Leader(leader) => {
                let current_n = self
                    .common
                    .last_issued_proposal
                    .expect("leader must have active proposal");
                let current_proposal_id = ProposalID(current_n, self.common.id);
                match msg.msg {
                    // messages we expect
                    Message::Promise(n, previously_accepted) => {
                        leader.handle_promise(&mut self.common, msg.from, n, previously_accepted)
                    }
                    Message::Accepted(n, slot) => {
                        leader.handle_accepted(&mut self.common, msg.from, n, slot)
                    }
                    Message::Nack(proposal_id) => {
                        // did we get pre-empted?
                        if proposal_id > current_proposal_id {
                            // switch to follower!
                            self.phase = Phase::Follower(Follower {
                                min_next_proposal_time: current_tick + PROPOSAL_COOLDOWN,
                            });
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
                            self.phase = Phase::Follower(Follower {
                                min_next_proposal_time: current_tick + PROPOSAL_COOLDOWN,
                            });
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
            Phase::Follower(follower) => {
                // cool down the proposal timer
                // TODO: should this only be for certain messages?
                follower.min_next_proposal_time = current_tick + PROPOSAL_COOLDOWN;
                match msg.msg {
                    // messages we expect
                    Message::Prepare(n, unchosen_gaps) => {
                        // check if this is newer than our current proposal
                        let reply = match follower
                            .make_promise_unless_obsolete(&mut self.common, ProposalID(n, msg.from))
                        {
                            // reply with all the entries that the leader requested
                            Ok(()) => {
                                Message::Promise(n, self.get_previously_accepted(&unchosen_gaps))
                            }
                            // NACK it
                            Err(latest_promised) => {
                                if ENABLE_NACKS {
                                    Message::Nack(latest_promised)
                                } else {
                                    return vec![];
                                }
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
                        let reply = match follower
                            .make_promise_unless_obsolete(&mut self.common, proposal_id)
                        {
                            Ok(()) => {
                                // record it in the log
                                self.common.log.accept_value(proposal_id, slot, value);
                                // tell the leader we accepted
                                Message::Accepted(n, slot)
                            }
                            // NACK the leader
                            Err(latest_promised) => {
                                if ENABLE_NACKS {
                                    Message::Nack(latest_promised)
                                } else {
                                    return vec![];
                                }
                            }
                        };
                        vec![Outgoing {
                            to: msg.from,
                            msg: reply,
                        }]
                    }
                    Message::Learned(_, slot, value) => {
                        // unconditional accept
                        self.common.log.commit_value(slot, value);
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

    fn get_previously_accepted(&self, unchosen_gaps: &Gaps) -> PreviouslyAccepted {
        // WARNING: this is an infinite iterator! map_while makes it finite
        unchosen_gaps
            .iter()
            .map_while(|slot| {
                let entry = self.common.log.get(slot)?;
                Some((slot, entry))
            })
            .filter_map(|(slot, entry)| {
                let x = match entry {
                    LogEntry::Empty => return None,
                    LogEntry::Accepted(proposal_id, value) => (Some(*proposal_id), value.clone()),
                    LogEntry::Committed(value) => (None, value.clone()),
                };
                Some((slot, x))
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

impl Leader {
    fn handle_promise(
        &mut self,
        common: &mut Common,
        from: ProcessID,
        n: usize,
        previously_accepted: PreviouslyAccepted,
    ) -> Vec<Outgoing<Message>> {
        let current_proposal_id = common
            .last_proposal_id()
            .expect("leader must have active proposal");

        // check that it's from our current proposal
        assert_le!(n, current_proposal_id.0);
        if n != current_proposal_id.0 {
            return vec![];
        }

        // if we already had quorum, don't re-send the accepts
        if self.promises_received.len() >= QUORUM {
            return vec![];
        }

        // add this response and see if we have quorum
        self.promises_received.insert(from, previously_accepted);
        if self.promises_received.len() < QUORUM {
            // no quorum yet
            return vec![];
        }

        log::trace!("Reached quorum for proposal {}", current_proposal_id);

        // figure out what values we should propose
        let mut values = self
            .promises_received
            .clone()
            .into_values()
            .flatten()
            .into_grouping_map()
            .max_by_key(|_slot, (proposal_id, _val)| {
                // silly hack: we want to take the largest proposal value, but None should
                // rank *higher* than any Some, not lower!
                // reversing the final result puts None at the top, but prefers lower
                // proposals, so we apply an inner Reverse as well
                std::cmp::Reverse(proposal_id.map(std::cmp::Reverse))
            });

        // now fill all our gaps until we've used up all the values we
        // need to propagate
        assert_eq!(self.uncommitted_slots, HashMap::new());
        let mut messages = vec![];
        for slot in self.unchosen_gaps.clone().iter() {
            if values.is_empty() {
                break;
            }

            // what did we find out about this slot?
            let msgs = match values.remove(&slot) {
                // nothing; no quorum has accepted any value yet, propose a no-op value
                None => self.start_accept_phase(
                    common,
                    current_proposal_id,
                    slot,
                    String::from("NO-OP"),
                ),
                // someone's accepted a value but it's not confirmed,
                // start an Accept round for it
                Some((Some(_), value)) => {
                    self.start_accept_phase(common, current_proposal_id, slot, value.clone())
                }
                // this value has been committed by someone, tell everyone
                // to learn it
                Some((None, value)) => {
                    common.log.commit_value(slot, value.clone());
                    common.msg_everybody_else(Message::Learned(n, slot, value))
                }
            };

            messages.extend(msgs);
        }

        messages
    }

    fn handle_accepted(
        &mut self,
        common: &mut Common,
        from: ProcessID,
        n: usize,
        slot: usize,
    ) -> Vec<Outgoing<Message>> {
        let current_proposal_id = common
            .last_proposal_id()
            .expect("leader must have active proposal");

        // check that it's from our current proposal
        assert_le!(n, current_proposal_id.0);
        if n != current_proposal_id.0 {
            return vec![];
        }

        // this should always be fine because we checked the proposal id above
        let (value, acceptors) = self
            .uncommitted_slots
            .get_mut(&slot)
            .expect("Accepted can only happen when we initiate it");
        let value = value.clone();

        // do we already have quorum? if so, ignore this
        if acceptors.len() >= QUORUM {
            return vec![];
        }

        // add this accept and see if we have quorum
        acceptors.insert(from);
        if acceptors.len() < QUORUM {
            // no quorum yet
            return vec![];
        }

        // great! we can commit this one
        common.log.commit_value(slot, value.clone());
        common.msg_everybody_else(Message::Learned(n, slot, value))
    }

    /// Begins a round of Phase 2, i.e., the sending of Accept() messages.
    /// Returns the Accept messages that should be sent, and simulates the
    /// effects of an Accept()/Accepted() request-response on ourselves.
    ///
    /// TODO: should we just create a simulated follower?
    fn start_accept_phase(
        &mut self,
        common: &mut Common,
        proposal_id: ProposalID,
        slot: usize,
        value: String,
    ) -> Vec<Outgoing<Message>> {
        // fake an Accept() message to self
        common.log.accept_value(proposal_id, slot, value.clone());

        // fake the Accepted() reply from self
        self.uncommitted_slots
            .insert(slot, (value.clone(), HashSet::from([common.id])));

        common.msg_everybody_else(Message::Accept(proposal_id.0, slot, value))
    }
}

impl Follower {
    /// Returns `Ok(())` and bumps internal state, unless we've already made a promise
    /// to a higher-numbered proposal, in which case we return an `Err` containing that
    /// proposal.
    fn make_promise_unless_obsolete(
        &mut self,
        common: &mut Common,
        proposal_id: ProposalID,
    ) -> Result<(), ProposalID> {
        if let Some(latest_promised) = &common.latest_promised {
            if *latest_promised > proposal_id {
                return Err(*latest_promised);
            }
        }

        // update
        common.latest_promised = Some(proposal_id);
        Ok(())
    }
}

impl Common {
    fn last_proposal_id(&self) -> Option<ProposalID> {
        self.last_issued_proposal.map(|n| ProposalID(n, self.id))
    }

    fn msg_everybody_else(&self, msg: Message) -> Vec<Outgoing<Message>> {
        (0..N)
            .filter_map(|i| {
                if i == self.id.0 {
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

    /// Returns a [Gaps] representing the set of uncommitted entries in the log.
    fn find_gaps(&self) -> Gaps {
        let interior = self
            .log
            .iter()
            .enumerate()
            .filter_map(|(slot, entry)| match entry {
                LogEntry::Empty | LogEntry::Accepted(_, _) => Some(slot),
                LogEntry::Committed(_) => None,
            })
            .collect();

        // TODO: contract?

        Gaps {
            interior,
            tail_start: self.log.len(),
        }
    }
}

impl Log {
    fn new() -> Self {
        Self(vec![])
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn iter(&self) -> impl Iterator<Item = &LogEntry> {
        self.0.iter()
    }

    fn get(&self, slot: usize) -> Option<&LogEntry> {
        self.0.get(slot)
    }

    fn accept_value(&mut self, proposal_id: ProposalID, slot: usize, value: String) {
        let entry = get_mut_extending_if_necessary(&mut self.0, slot, || LogEntry::Empty);
        // sanity check
        match entry {
            LogEntry::Empty => {}
            LogEntry::Accepted(old_proposal_id, _) => {
                assert_ge!(proposal_id, *old_proposal_id);
            }
            LogEntry::Committed(chosen_value) => {
                assert_eq!(value, *chosen_value);
            }
        }
        *entry = LogEntry::Accepted(proposal_id, value);
    }

    fn commit_value(&mut self, slot: usize, value: String) {
        // unconditional accept
        let entry = get_mut_extending_if_necessary(&mut self.0, slot, || LogEntry::Empty);
        // sanity check
        match entry {
            LogEntry::Empty => {}
            LogEntry::Accepted(_, _) => {}
            LogEntry::Committed(chosen_value) => {
                assert_eq!(value, *chosen_value);
            }
        }
        *entry = LogEntry::Committed(value);
    }
}

impl Gaps {
    fn iter(&self) -> impl Iterator<Item = usize> + use<'_> {
        self.interior.iter().cloned().chain(self.tail_start..)
    }
}
