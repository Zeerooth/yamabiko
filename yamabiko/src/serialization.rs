use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str::FromStr};

use crate::error::InvalidDataFormatError;
use crate::field::Field;

#[cfg(any(feature = "yaml", feature = "full"))]
use serde_yml;

#[derive(Debug, Clone, Copy)]
pub enum DataFormat {
    Json,
    #[cfg(any(feature = "yaml", feature = "full"))]
    Yaml,
}

impl FromStr for DataFormat {
    type Err = InvalidDataFormatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let normalized_str = s.to_lowercase();
        match normalized_str.as_str() {
            "json" => Ok(Self::Json),
            #[cfg(any(feature = "yaml", feature = "full"))]
            "yaml" => Ok(Self::Yaml),
            _ => Err(InvalidDataFormatError),
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
                if let Some(field) = Field::from_json_value(index_value) {
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
                if let Some(field) = Field::from_yaml_value(index_value) {
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
    ) -> String {
        match self {
            Self::Json => {
                let v: serde_json::Value = serde_json::from_slice(data).unwrap();
                DataFormat::extract_indexes_json(&v, indexes);
                serde_json::to_string_pretty(&v).unwrap()
            }
            #[cfg(any(feature = "yaml", feature = "full"))]
            Self::Yaml => {
                let v: serde_yml::Value = serde_yml::from_slice(data).unwrap();
                DataFormat::extract_indexes_yaml(&v, indexes);
                serde_yml::to_string(&v).unwrap()
            }
        }
    }

    pub fn serialize_with_indexes<T>(
        &self,
        data: T,
        indexes: &mut HashMap<&crate::index::Index, Option<Field>>,
    ) -> String
    where
        T: Serialize,
    {
        match self {
            Self::Json => {
                let v: serde_json::Value = serde_json::to_value(&data).unwrap();
                DataFormat::extract_indexes_json(&v, indexes);
                serde_json::to_string_pretty(&v).unwrap()
            }
            #[cfg(any(feature = "yaml", feature = "full"))]
            Self::Yaml => {
                let v: serde_yml::Value = serde_yml::to_value(&data).unwrap();
                DataFormat::extract_indexes_yaml(&v, indexes);
                serde_yml::to_string(&v).unwrap()
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
        }
    }

    pub fn deserialize<'a, T>(&self, data: &'a str) -> T
    where
        T: Deserialize<'a>,
    {
        match self {
            Self::Json => serde_json::from_str(data).unwrap(),
            #[cfg(any(feature = "yaml", feature = "full"))]
            Self::Yaml => serde_yml::from_str(data).unwrap(),
        }
    }
}
