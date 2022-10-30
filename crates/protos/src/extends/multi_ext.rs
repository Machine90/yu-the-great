use prost::{bytes::BytesMut, Message};
use serde::{de, ser, Deserialize};

use crate::multi_proto::{BatchMessages, MultiGroup, Assignments, BatchAssignment};

impl serde::Serialize for BatchMessages {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut buf = BytesMut::new();
        let e = self.encode(&mut buf);
        if let Err(e) = e {
            return Err(ser::Error::custom(e));
        }
        let ser = serializer.serialize_bytes(&buf[..])?;
        Ok(ser)
    }
}

impl<'de> Deserialize<'de> for BatchMessages {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buf: Vec<u8> = Deserialize::deserialize(deserializer)?;
        let d = BatchMessages::decode(&buf[..]);
        if let Err(e) = d {
            return Err(de::Error::custom(e));
        }
        Ok(d.unwrap())
    }
}

impl serde::Serialize for MultiGroup {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut buf = BytesMut::new();
        let e = self.encode(&mut buf);
        if let Err(e) = e {
            return Err(ser::Error::custom(e));
        }
        let ser = serializer.serialize_bytes(&buf[..])?;
        Ok(ser)
    }
}

impl<'de> Deserialize<'de> for MultiGroup {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buf: Vec<u8> = Deserialize::deserialize(deserializer)?;
        let d = MultiGroup::decode(&buf[..]);
        if let Err(e) = d {
            return Err(de::Error::custom(e));
        }
        Ok(d.unwrap())
    }
}

impl Assignments {

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.should_split.is_empty() && self.should_merge.is_empty() && self.should_transfer.is_empty()
    }
}

impl serde::Serialize for Assignments {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut buf = BytesMut::new();
        let e = self.encode(&mut buf);
        if let Err(e) = e {
            return Err(ser::Error::custom(e));
        }
        let ser = serializer.serialize_bytes(&buf[..])?;
        Ok(ser)
    }
}

impl<'de> Deserialize<'de> for Assignments {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buf: Vec<u8> = Deserialize::deserialize(deserializer)?;
        let d = Assignments::decode(&buf[..]);
        if let Err(e) = d {
            return Err(de::Error::custom(e));
        }
        Ok(d.unwrap())
    }
}

impl serde::Serialize for BatchAssignment {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut buf = BytesMut::new();
        let e = self.encode(&mut buf);
        if let Err(e) = e {
            return Err(ser::Error::custom(e));
        }
        let ser = serializer.serialize_bytes(&buf[..])?;
        Ok(ser)
    }
}

impl<'de> Deserialize<'de> for BatchAssignment {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buf: Vec<u8> = Deserialize::deserialize(deserializer)?;
        let d = BatchAssignment::decode(&buf[..]);
        if let Err(e) = d {
            return Err(de::Error::custom(e));
        }
        Ok(d.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    #[test]
    fn test_cmd() {
        let v1 = vec![1, 2, 3];
        let v2 = vec![1, 2, 3];
        let mut mp = HashMap::new();
        mp.insert(&v1, 1);
        let eq = mp.contains_key(&v2);
        println!("eq? {eq}");
    }
}
