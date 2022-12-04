use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub enum DataFormat {
    Json,
    #[cfg(feature = "yaml")]
    Yaml,
}

impl DataFormat {
    pub fn extract_indexes_json(
        data: &serde_json::Value,
        indexes: &mut HashMap<String, Option<String>>,
    ) {
        for (k, v) in indexes.iter_mut() {
            if let Some(index_value) = data.get(k) {
                *v = Some(index_value.to_string())
            }
        }
    }

    pub fn serialize_with_indexes<T>(
        &self,
        data: T,
        mut indexes: HashMap<String, Option<String>>,
    ) -> String
    where
        T: Serialize,
    {
        match self {
            Self::Json => {
                let v: serde_json::Value = serde_json::to_value(&data).unwrap();
                DataFormat::extract_indexes_json(&v, &mut indexes);
                serde_json::to_string(&v).unwrap()
            }
            #[cfg(feature = "yaml")]
            Self::Yaml => serde_yaml::to_string(&data).unwrap(),
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
