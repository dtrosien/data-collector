
use std::hash::Hash;

pub trait MyTest {}

pub trait MyEquals {}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct T1 {
    api_key: String,
}

impl MyTest for T1 {}

// #############################
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct T2 {
    api_key: String,
}

impl MyTest for T2 {}

// #[derive(Debug)]
pub struct KeyChain {
    status: u8,
    key: Box<dyn MyTest>,
}

impl PartialEq for KeyChain {
    fn eq(&self, other: &Self) -> bool {
        self.status == other.status
    }
}
impl Eq for KeyChain {}

impl Hash for KeyChain {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.status.hash(state);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TestEnum {
    Financialmodelingprep,
    Polygon,
}
