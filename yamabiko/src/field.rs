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

#[cfg(any(feature = "yaml", feature = "full"))]
impl PartialEq<serde_yml::Value> for Field {
    fn eq(&self, other: &serde_yml::Value) -> bool {
        match self {
            Field::Float(f) => other.as_f64().map(|x| &x == f).unwrap_or(false),
            Field::Int(i) => other.as_i64().map(|x| &x == i).unwrap_or(false),
            Field::String(s) => other.as_str().map(|x| x == s).unwrap_or(false),
        }
    }
}

#[cfg(any(feature = "yaml", feature = "full"))]
impl PartialOrd<serde_yml::Value> for Field {
    fn partial_cmp(&self, other: &serde_yml::Value) -> Option<Ordering> {
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

#[cfg(any(feature = "pot", feature = "full"))]
impl<'a> PartialEq<pot::Value<'a>> for Field {
    fn eq(&self, other: &pot::Value) -> bool {
        match self {
            Field::Float(f) => other.as_float().map(|x| &x.as_f64() == f).unwrap_or(false),
            Field::Int(i) => other
                .as_integer()
                .map(|x| &x.as_i64().unwrap() == i)
                .unwrap_or(false),
            Field::String(s) => other.as_str().map(|x| x == s).unwrap_or(false),
        }
    }
}

#[cfg(any(feature = "pot", feature = "full"))]
impl<'a> PartialOrd<pot::Value<'a>> for Field {
    fn partial_cmp(&self, other: &pot::Value) -> Option<Ordering> {
        match self {
            Field::Float(f) => other
                .as_float()
                .map(|x| x.as_f64().partial_cmp(f))
                .unwrap_or(None),
            Field::Int(i) => other
                .as_integer()
                .map(|x| x.as_i64().unwrap().partial_cmp(i))
                .unwrap_or(None),
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
}

impl TryFrom<&serde_json::Value> for Field {
    type Error = ();

    fn try_from(value: &serde_json::Value) -> Result<Self, Self::Error> {
        match value {
            serde_json::Value::Null => todo!(),
            serde_json::Value::Bool(_) => todo!(),
            serde_json::Value::Number(v) => v
                .as_i64()
                .map(Self::Int)
                .or_else(|| v.as_f64().map(Self::Float))
                .ok_or(()),
            serde_json::Value::String(v) => Ok(Self::String(v.as_str().to_string())),
            serde_json::Value::Array(_) => todo!(),
            serde_json::Value::Object(_) => todo!(),
        }
    }
}

#[cfg(any(feature = "yaml", feature = "full"))]
impl TryFrom<&serde_yml::Value> for Field {
    type Error = ();

    fn try_from(value: &serde_yml::Value) -> Result<Self, Self::Error> {
        match value {
            serde_yml::Value::Null => todo!(),
            serde_yml::Value::Bool(_) => todo!(),
            serde_yml::Value::Number(v) => v
                .as_i64()
                .map(Self::Int)
                .or_else(|| v.as_f64().map(Self::Float))
                .ok_or(()),
            serde_yml::Value::String(v) => Ok(Self::String(v.as_str().to_string())),
            serde_yml::Value::Sequence(_vec) => todo!(),
            serde_yml::Value::Mapping(_mapping) => todo!(),
            serde_yml::Value::Tagged(_tagged_value) => todo!(),
        }
    }
}

#[cfg(any(feature = "pot", feature = "full"))]
impl<'a> TryFrom<&pot::Value<'a>> for Field {
    type Error = ();

    fn try_from(value: &pot::Value) -> Result<Self, Self::Error> {
        match value {
            pot::Value::None => todo!(),
            pot::Value::Unit => todo!(),
            pot::Value::Bool(_) => todo!(),
            pot::Value::Integer(i) => i.as_i64().map(Self::Int).map_err(|_| ()),
            pot::Value::Float(f) => Ok(Self::Float(f.as_f64())),
            pot::Value::Bytes(_cow) => todo!(),
            pot::Value::String(s) => Ok(Self::String(s.to_string())),
            pot::Value::Sequence(_vec) => todo!(),
            pot::Value::Mappings(_vec) => todo!(),
        }
    }
}
