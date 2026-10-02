//! Conversions between Python objects and Rust values (JSON, string maps,
//! byte maps).

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyList, PyString};
use std::collections::HashMap;

/// Convert a `serde_json::Value` to a Python object.
pub fn json_value_to_py(py: Python<'_>, value: &serde_json::Value) -> PyResult<PyObject> {
    match value {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(b) => Ok(PyBool::new(py, *b).to_owned().into_any().unbind()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py)?.into_any().unbind())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py)?.into_any().unbind())
            } else {
                Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid number",
                ))
            }
        }
        serde_json::Value::String(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
        serde_json::Value::Array(arr) => {
            let list = PyList::empty(py);
            for item in arr {
                list.append(json_value_to_py(py, item)?)?;
            }
            Ok(list.into_pyobject(py)?.into_any().unbind())
        }
        serde_json::Value::Object(obj) => {
            let dict = PyDict::new(py);
            for (key, val) in obj {
                dict.set_item(key, json_value_to_py(py, val)?)?;
            }
            Ok(dict.into_pyobject(py)?.into_any().unbind())
        }
    }
}

/// Convert a Python object to a `serde_json::Value`.
pub fn py_to_json_value(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    if obj.is_none() {
        return Ok(serde_json::Value::Null);
    }

    if let Ok(b) = obj.downcast::<PyBool>() {
        return Ok(serde_json::Value::Bool(b.is_true()));
    }

    if let Ok(i) = obj.extract::<i64>() {
        return Ok(serde_json::Value::Number(i.into()));
    }

    if let Ok(f) = obj.extract::<f64>() {
        return Ok(serde_json::json!(f));
    }

    if let Ok(s) = obj.downcast::<PyString>() {
        return Ok(serde_json::Value::String(s.to_string()));
    }

    if let Ok(bytes) = obj.downcast::<PyBytes>() {
        // JSON has no bytes type, so bytes become a standard base64 string.
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode(bytes.as_bytes());
        return Ok(serde_json::Value::String(encoded));
    }

    if let Ok(list) = obj.downcast::<PyList>() {
        let mut arr = Vec::new();
        for item in list.iter() {
            arr.push(py_to_json_value(py, &item)?);
        }
        return Ok(serde_json::Value::Array(arr));
    }

    if let Ok(dict) = obj.downcast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (key, value) in dict.iter() {
            let key_str: String = key.extract()?;
            map.insert(key_str, py_to_json_value(py, &value)?);
        }
        return Ok(serde_json::Value::Object(map));
    }

    // Anything else goes through Python's json module, so any object it can
    // serialize still converts.
    let json_module = py.import("json")?;
    let json_str: String = json_module.call_method1("dumps", (obj,))?.extract()?;
    serde_json::from_str(&json_str).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to convert to JSON: {}", e))
    })
}

/// Convert a Python dict to a `HashMap<String, String>`.
pub fn py_dict_to_string_map(dict: &Bound<'_, PyDict>) -> PyResult<HashMap<String, String>> {
    let mut map = HashMap::new();
    for (key, value) in dict.iter() {
        let key_str: String = key.extract()?;
        let value_str: String = value.extract()?;
        map.insert(key_str, value_str);
    }
    Ok(map)
}

/// Convert a Python dict to a `HashMap<String, Vec<u8>>`. Values may be
/// bytes or str; str values are stored as their UTF-8 bytes.
pub fn py_dict_to_bytes_map(dict: &Bound<'_, PyDict>) -> PyResult<HashMap<String, Vec<u8>>> {
    let mut map = HashMap::new();
    for (key, value) in dict.iter() {
        let key_str: String = key.extract()?;
        let value_bytes: Vec<u8> = if let Ok(bytes) = value.downcast::<PyBytes>() {
            bytes.as_bytes().to_vec()
        } else if let Ok(s) = value.extract::<String>() {
            s.into_bytes()
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Values must be bytes or str",
            ));
        };
        map.insert(key_str, value_bytes);
    }
    Ok(map)
}

/// Convert a `HashMap<String, Vec<u8>>` to a Python dict of bytes.
pub fn bytes_map_to_py_dict<'py>(
    py: Python<'py>,
    map: &HashMap<String, Vec<u8>>,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (key, value) in map {
        dict.set_item(key, PyBytes::new(py, value))?;
    }
    Ok(dict)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::prepare_freethreaded_python;

    #[test]
    fn test_json_value_to_py_null() {
        prepare_freethreaded_python();
        Python::with_gil(|py| {
            let result = json_value_to_py(py, &serde_json::Value::Null).unwrap();
            assert!(result.is_none(py));
        });
    }

    #[test]
    fn test_json_value_to_py_string() {
        prepare_freethreaded_python();
        Python::with_gil(|py| {
            let value = serde_json::Value::String("hello".to_string());
            let result = json_value_to_py(py, &value).unwrap();
            let s: String = result.extract(py).unwrap();
            assert_eq!(s, "hello");
        });
    }

    #[test]
    fn test_json_value_to_py_number() {
        prepare_freethreaded_python();
        Python::with_gil(|py| {
            let value = serde_json::json!(42);
            let result = json_value_to_py(py, &value).unwrap();
            let n: i64 = result.extract(py).unwrap();
            assert_eq!(n, 42);
        });
    }

    #[test]
    fn test_json_value_to_py_object() {
        prepare_freethreaded_python();
        Python::with_gil(|py| {
            let value = serde_json::json!({"key": "value", "num": 123});
            let result = json_value_to_py(py, &value).unwrap();
            let dict = result.downcast_bound::<PyDict>(py).unwrap();
            let key_val: String = dict.get_item("key").unwrap().unwrap().extract().unwrap();
            assert_eq!(key_val, "value");
        });
    }
}
