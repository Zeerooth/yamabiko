use serde::{Deserialize, Serialize};

pub enum DataFormat {
    Json,
    #[cfg(feature = "yaml")]
    Yaml,
}

impl DataFormat {
    pub fn serialize<T>(&self, data: T) -> String
    where
        T: Serialize,
    {
        match self {
            Self::Json => serde_json::to_string(&data).unwrap(),
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
