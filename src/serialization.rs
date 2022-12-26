use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Copy)]
pub enum DataFormat {
    Json,
    #[cfg(feature = "yaml")]
    Yaml,
}

impl DataFormat {
    pub fn extract_indexes_json(
        data: &serde_json::Value,
        indexes: &mut HashMap<&crate::index::Index, Option<String>>,
    ) {
        for (k, v) in indexes.iter_mut() {
            if let Some(index_value) = data.get(k.indexed_field()) {
                *v = Some(index_value.as_str().unwrap().to_string())
            }
        }
    }

    pub fn serialize_with_indexes<T>(
        &self,
        data: T,
        indexes: &mut HashMap<&crate::index::Index, Option<String>>,
    ) -> String
    where
        T: Serialize,
    {
        match self {
            Self::Json => {
                let v: serde_json::Value = serde_json::to_value(&data).unwrap();
                DataFormat::extract_indexes_json(&v, indexes);
                serde_json::to_string(&v).unwrap()
            }
            #[cfg(feature = "yaml")]
            Self::Yaml => serde_yaml::to_string(&data).unwrap(),
        }
    }

    pub fn match_field(
        &self,
        data: &[u8],
        field: &str,
        value: &str,
        comparison: std::cmp::Ordering,
    ) -> bool {
        match self {
            Self::Json => {
                let v: serde_json::Value = serde_json::from_slice(&data).unwrap();
                match v.get(field) {
                    Some(res) => {
                        let other = res
                            .as_str()
                            .map(|x| x.to_string())
                            .unwrap_or_else(|| res.to_string());
                        other.as_str().cmp(value) == comparison
                    }
                    None => false,
                }
            }
            #[cfg(feature = "yaml")]
            Self::Yaml => true,
        }
    }

    pub fn deserialize<'a, T>(&self, data: &'a str) -> T
    where
        T: Deserialize<'a>,
    {
        match self {
            Self::Json => serde_json::from_str(&data).unwrap(),
            #[cfg(feature = "yaml")]
            Self::Yaml => serde_yaml::from_str(&data).unwrap(),
        }
    }
}
