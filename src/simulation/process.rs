use super::Context;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProcessID(pub usize);

// TODO: generalize message and consensus types!
pub trait Process {
    type Message: std::fmt::Debug;

    fn new(id: ProcessID) -> Self;
    fn tick(&mut self, ctx: Context<Self::Message>);
    fn crash(&mut self);
    fn status(&self) -> String;
    fn decided_value(&self) -> Option<&String>;
}
