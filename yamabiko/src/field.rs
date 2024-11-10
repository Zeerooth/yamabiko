use std::cmp::Ordering;

use git2::IndexEntry;

use crate::index::Index;

#[derive(Debug, PartialEq)]
pub enum Field {
    Int(i64),
    Float(f64),
    String(String),
}

impl From<f64> for Field {
    fn from(number: f64) -> Self {
        Self::Float(number)
    }
}

impl From<i64> for Field {
    fn from(number: i64) -> Self {
        Self::Int(number)
    }
}

impl From<String> for Field {
    fn from(text: String) -> Self {
        Self::String(text)
    }
}

impl From<&str> for Field {
    fn from(text: &str) -> Self {
        Self::String(text.to_owned())
    }
}

impl PartialEq<serde_json::Value> for Field {
    fn eq(&self, other: &serde_json::Value) -> bool {
        match self {
            Field::Float(f) => other.as_f64().map(|x| &x == f).unwrap_or(false),
            Field::Int(i) => other.as_i64().map(|x| &x == i).unwrap_or(false),
            Field::String(s) => other.as_str().map(|x| x == s).unwrap_or(false),
        }
    }
}

impl PartialOrd<serde_json::Value> for Field {
    fn partial_cmp(&self, other: &serde_json::Value) -> Option<Ordering> {
        match self {
            Field::Float(f) => other.as_f64().map(|x| x.partial_cmp(f)).unwrap_or(None),
            Field::Int(i) => other.as_i64().map(|x| x.partial_cmp(i)).unwrap_or(None),
            Field::String(s) => other
                .as_str()
                .map(|x| x.partial_cmp(s.as_str()))
                .unwrap_or(None),
        }
    }
}

impl PartialOrd<Self> for Field {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            Field::Float(sf) => match other {
                Field::Int(oi) => (*oi as f64).partial_cmp(sf),
                Field::Float(of) => of.partial_cmp(sf),
                Field::String(_) => None,
            },
            Field::Int(si) => match other {
                Field::Int(oi) => oi.partial_cmp(si),
                Field::Float(of) => (of).partial_cmp(&(*si as f64)),
                Field::String(_) => None,
            },
            Field::String(ss) => match other {
                Field::String(os) => os.partial_cmp(ss),
                _ => None,
            },
        }
    }
}

impl ToString for Field {
    fn to_string(&self) -> String {
        match self {
            Self::Int(v) => v.to_string(),
            Self::String(v) => v.to_string(),
            Self::Float(v) => v.to_string(),
        }
    }
}

impl Field {
    /// Extract a value `git2::index::IndexEntry` into a `Field`
    ///
    /// This is done based on the `ino` value
    ///
    /// Returns `None` if the convesion cannot be performed
    pub fn from_index_entry(index_entry: &IndexEntry) -> Option<Self> {
        let val = String::from_utf8_lossy(Index::extract_value(index_entry));
        match index_entry.ino {
            0 => Some(Self::from(
                f64::from_bits(u64::from_str_radix(&val, 16).ok()?) as i64,
            )),
            1 => Some(Self::from(val.to_string())),
            2 => Some(Self::from(f64::from_bits(
                u64::from_str_radix(&val, 16).ok()?,
            ))),
            _ => None,
        }
    }

    pub fn to_index_value(&self) -> String {
        match self {
            Field::Int(v) => format!(
                "{}/{:16x}",
                match v.is_positive() {
                    true => "1",
                    false => "0",
                },
                (*v as f64).to_bits()
            ),
            Field::Float(v) => format!(
                "{}/{:16x}",
                match v.is_sign_positive() {
                    true => "1",
                    false => "0",
                },
                v.to_bits()
            ),
            Field::String(v) => v.to_owned(),
        }
    }

    pub fn to_ino_number(&self) -> u32 {
        match self {
            Field::Int(_) => 0,
            Field::Float(_) => 2,
            Field::String(_) => 1,
        }
    }

    pub fn from_json_value(value: &serde_json::Value) -> Option<Self> {
        match value {
            serde_json::Value::Null => todo!(),
            serde_json::Value::Bool(_) => todo!(),
            serde_json::Value::Number(v) => v
                .as_i64()
                .map(Self::Int)
                .or_else(|| v.as_f64().map(Self::Float)),
            serde_json::Value::String(v) => Some(Self::String(v.as_str().to_string())),
            serde_json::Value::Array(_) => todo!(),
            serde_json::Value::Object(_) => todo!(),
        }
    }
}
