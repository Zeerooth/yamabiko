use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::{collections::HashMap, str::FromStr};

use crate::error::InvalidDataFormatError;
use crate::field::Field;

#[cfg(any(feature = "yaml", feature = "full"))]
use serde_yml;

#[derive(Debug, Clone, Copy)]
pub enum DataFormat {
    /// The default. Wide support, human-readable, rather fast.
    Json,

    #[cfg(any(feature = "yaml", feature = "full"))]
    /// Good alternative to JSON for even better readability.
    Yaml,

    #[cfg(any(feature = "pot", feature = "full"))]
    /// Binary, compact and fast data format. Saves space. Not human-readable.
    Pot,
}

impl FromStr for DataFormat {
    type Err = InvalidDataFormatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let normalized_str = s.to_lowercase();
        match normalized_str.as_str() {
            "json" => Ok(Self::Json),
            #[cfg(any(feature = "yaml", feature = "full"))]
            "yaml" => Ok(Self::Yaml),
            #[cfg(any(feature = "pot", feature = "full"))]
            "pot" => Ok(Self::Pot),
            _ => Err(InvalidDataFormatError),
        }
    }
}

impl Display for DataFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataFormat::Json => write!(f, "json"),
            #[cfg(any(feature = "yaml", feature = "full"))]
            DataFormat::Yaml => write!(f, "yaml"),
            #[cfg(any(feature = "pot", feature = "full"))]
            DataFormat::Pot => write!(f, "pot"),
        }
    }
}

impl DataFormat {
    pub fn extract_indexes_json(
        data: &serde_json::Value,
        indexes: &mut HashMap<&crate::index::Index, Option<Field>>,
    ) {
        for (k, v) in indexes.iter_mut() {
            if let Some(index_value) = data.get(k.indexed_field()) {
                if let Ok(field) = Field::try_from(index_value) {
                    if k.indexes_given_field(&field) {
                        *v = Some(field);
                    }
                }
            }
        }
    }

    #[cfg(any(feature = "yaml", feature = "full"))]
    pub fn extract_indexes_yaml(
        data: &serde_yml::Value,
        indexes: &mut HashMap<&crate::index::Index, Option<Field>>,
    ) {
        for (k, v) in indexes.iter_mut() {
            if let Some(index_value) = data.get(k.indexed_field()) {
                if let Ok(field) = Field::try_from(index_value) {
                    if k.indexes_given_field(&field) {
                        *v = Some(field);
                    }
                }
            }
        }
    }

    #[cfg(any(feature = "pot", feature = "full"))]
    pub fn extract_indexes_pot(
        data: &pot::Value,
        indexes: &mut HashMap<&crate::index::Index, Option<Field>>,
    ) {
        for (k, v) in indexes.iter_mut() {
            if let Some(index_value) = data
                .mappings()
                .find(|m| m.0 == pot::Value::from(k.indexed_field()))
            {
                if let Ok(field) = Field::try_from(&index_value.1) {
                    if k.indexes_given_field(&field) {
                        *v = Some(field);
                    }
                }
            }
        }
    }

    pub fn serialize_with_indexes_raw(
        &self,
        data: &[u8],
        indexes: &mut HashMap<&crate::index::Index, Option<Field>>,
    ) -> Vec<u8> {
        match self {
            Self::Json => {
                let v: serde_json::Value = serde_json::from_slice(data).unwrap();
                DataFormat::extract_indexes_json(&v, indexes);
                serde_json::to_vec(&v).unwrap()
            }
            #[cfg(any(feature = "yaml", feature = "full"))]
            Self::Yaml => {
                let v: serde_yml::Value = serde_yml::from_slice(data).unwrap();
                DataFormat::extract_indexes_yaml(&v, indexes);
                serde_yml::to_string(&v).unwrap().as_bytes().to_owned()
            }
            #[cfg(any(feature = "pot", feature = "full"))]
            Self::Pot => {
                let v: pot::Value = pot::from_slice(data).unwrap();
                DataFormat::extract_indexes_pot(&v, indexes);
                pot::to_vec(&v).unwrap()
            }
        }
    }

    pub fn serialize_with_indexes<T>(
        &self,
        data: T,
        indexes: &mut HashMap<&crate::index::Index, Option<Field>>,
    ) -> Vec<u8>
    where
        T: Serialize,
    {
        match self {
            Self::Json => {
                let v: serde_json::Value = serde_json::to_value(&data).unwrap();
                DataFormat::extract_indexes_json(&v, indexes);
                serde_json::to_vec(&v).unwrap()
            }
            #[cfg(any(feature = "yaml", feature = "full"))]
            Self::Yaml => {
                let v: serde_yml::Value = serde_yml::to_value(&data).unwrap();
                DataFormat::extract_indexes_yaml(&v, indexes);
                serde_yml::to_string(&v).unwrap().as_bytes().to_owned()
            }
            #[cfg(any(feature = "pot", feature = "full"))]
            Self::Pot => {
                let vec = pot::to_vec(&data).unwrap();
                let v = pot::from_slice(&vec).unwrap();
                DataFormat::extract_indexes_pot(&v, indexes);
                vec
            }
        }
    }

    pub fn match_field(
        &self,
        data: &[u8],
        field: &str,
        value: &Field,
        comparison: std::cmp::Ordering,
    ) -> bool {
        match self {
            Self::Json => {
                let v: serde_json::Value = serde_json::from_slice(data).unwrap();
                match v.get(field) {
                    Some(res) => value.partial_cmp(res) == Some(comparison),
                    None => false,
                }
            }
            #[cfg(any(feature = "yaml", feature = "full"))]
            Self::Yaml => {
                let v: serde_yml::Value = serde_yml::from_slice(data).unwrap();
                match v.get(field) {
                    Some(res) => value.partial_cmp(res) == Some(comparison),
                    None => false,
                }
            }
            #[cfg(any(feature = "pot", feature = "full"))]
            Self::Pot => {
                let v: pot::Value = pot::from_slice(data).unwrap();
                match v.mappings().find(|m| m.0 == pot::Value::from(field)) {
                    Some(res) => value.partial_cmp(&res.1) == Some(comparison),
                    None => false,
                }
            }
        }
    }

    pub fn deserialize<'a, T>(&self, data: &'a [u8]) -> T
    where
        T: Deserialize<'a>,
    {
        match self {
            Self::Json => serde_json::from_slice(data).unwrap(),
            #[cfg(any(feature = "yaml", feature = "full"))]
            Self::Yaml => serde_yml::from_slice(data).unwrap(),
            #[cfg(any(feature = "pot", feature = "full"))]
            Self::Pot => pot::from_slice(data).unwrap(),
        }
    }
}
