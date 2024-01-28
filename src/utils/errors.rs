use std::error::Error;
use std::fmt;

pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

/// Simple Error with arbitrary String input
#[derive(Debug, Clone)]
pub struct SimpleError(String);
impl Error for SimpleError {}

impl fmt::Display for SimpleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Parser Error with input
#[derive(Debug, Clone)]
pub struct ParserError {
    input: String,
}
impl Error for ParserError {}

impl fmt::Display for ParserError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "cannot parse item: {}", self.input)
    }
}

/// Match Error todo: think about input type
#[derive(Debug, Clone)]
pub struct MatchError;

impl Error for MatchError {}

impl fmt::Display for MatchError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "cannot match item")
    }
}
