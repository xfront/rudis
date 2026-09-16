extern crate bytes;
extern crate parser;

use std::fmt::{Debug, Error, Formatter};
use std::sync::mpsc::Receiver;

use bytes::BytesMut;
use parser::OwnedParsedCommand;

/// A command response to send to a client
#[derive(PartialEq, Debug)]
pub enum Response {
    /// No data
    Nil,
    /// A number
    Integer(i64),
    /// Binary data
    Data(Vec<u8>),
    /// A simple error string
    Error(String),
    /// A simple status string
    Status(String),
    /// An array of responses that may mix different types
    Array(Vec<Response>),
}

/// No response was issued
pub enum ResponseError {
    /// The command generated no response
    NoReply,
    /// The command generated no response yet. At a later time, a new command
    /// should be executed, or give up if a None is received.
    /// Only one message will be sent.
    Wait(Receiver<Option<OwnedParsedCommand>>),
}

impl Debug for ResponseError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        match self {
            ResponseError::NoReply => write!(f, "NoReply"),
            ResponseError::Wait(_) => write!(f, "Wait"),
        }
    }
}

impl Response {
    /// Serializes the response into an array of bytes using Redis protocol.
    /// Uses BytesMut for efficient buffer building (Dragonfly-inspired optimization).
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut buf = BytesMut::with_capacity(64);
        self.write_to(&mut buf);
        buf.to_vec()
    }

    /// Writes the serialized response directly into a BytesMut buffer.
    /// This avoids intermediate allocations compared to as_bytes().
    pub fn write_to(&self, buf: &mut BytesMut) {
        match self {
            Response::Nil => buf.extend_from_slice(b"$-1\r\n"),
            Response::Data(d) => {
                buf.extend_from_slice(b"$");
                let mut len_buf = itoa::Buffer::new();
                buf.extend_from_slice(len_buf.format(d.len()).as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(d);
                buf.extend_from_slice(b"\r\n");
            }
            Response::Integer(i) => {
                buf.extend_from_slice(b":");
                let mut len_buf = itoa::Buffer::new();
                buf.extend_from_slice(len_buf.format(*i).as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Response::Error(d) => {
                buf.extend_from_slice(b"-");
                buf.extend_from_slice(d.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Response::Status(d) => {
                buf.extend_from_slice(b"+");
                buf.extend_from_slice(d.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Response::Array(a) => {
                buf.extend_from_slice(b"*");
                let mut len_buf = itoa::Buffer::new();
                buf.extend_from_slice(len_buf.format(a.len()).as_bytes());
                buf.extend_from_slice(b"\r\n");
                for el in a {
                    el.write_to(buf);
                }
            }
        }
    }

    /// Returns true if and only if the response is an error.
    pub fn is_error(&self) -> bool {
        if let Response::Error(_) = *self {
            true
        } else {
            false
        }
    }

    /// Is the response a status
    pub fn is_status(&self) -> bool {
        if let Response::Status(_) = *self {
            true
        } else {
            false
        }
    }
}
