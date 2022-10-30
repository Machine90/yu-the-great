use std::fmt::Display;

use prost::{Message};

use crate::{raft_log_proto::Entry};

impl From<Vec<u8>> for Entry {
    fn from(buffer: Vec<u8>) -> Self {
        let ent = Entry::decode(buffer.as_slice());
        if let Err(err) = ent {
            panic!("failed to decode entry, see error: {:?}", err);
        }
        ent.unwrap()
    }
}

impl Display for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut basic = format!(
            "[index: {:?}, term: {:?}, type: {:?}, sync_log: {:?}",
            self.index,
            self.term,
            self.entry_type(),
            self.sync_log
        );
        if self.data.len() > 1024 {
            basic.push(']');
            write!(f, "{}", basic)
        } else {
            let data = self.data.clone();
            let content = String::from_utf8(data);
            if content.is_ok() {
                basic.push_str(", data: ");
                basic += &content.unwrap();
                basic.push(']');
                write!(f, "{}", basic)
            } else {
                basic.push(']');
                write!(f, "{}", basic)
            }
        }
    }
}
