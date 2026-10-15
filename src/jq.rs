//! jq filters (powered by `jaq`).
//!
//! jq filters are used to:
//! - filter and transform the records returned by a records iterator,
//! - filter the streams by their properties,
//! - express ABAC rules on stream properties.
//!
//! Compiled `jaq` filters are not thread safe, therefore [`JqFilter`] only holds the
//! (validated) source code and compiled filters are cached per thread.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

use jaq_core::load::{Arena, File, Loader};
use jaq_core::{Ctx, Native, RcIter};
use jaq_json::Val;
use serde_json::Value;

type CompiledFilter = jaq_core::Filter<Native<Val>>;

/// Maximum count of compiled filters cached per thread.
const CACHE_CAPACITY: usize = 1024;

thread_local! {
    static CACHE: RefCell<HashMap<Arc<str>, Rc<CompiledFilter>>> = RefCell::new(HashMap::new());
}

/// A validated jq filter.
#[derive(Clone)]
pub struct JqFilter {
    source: Arc<str>,
}

impl fmt::Debug for JqFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JqFilter({:?})", self.source)
    }
}

impl fmt::Display for JqFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.source)
    }
}

impl JqFilter {
    /// Parse and compile a jq filter.
    pub fn parse(code: &str) -> Result<JqFilter, String> {
        let source: Arc<str> = Arc::from(code);
        let compiled = compile(code)?;
        CACHE.with(|cache| {
            insert_in_cache(&mut cache.borrow_mut(), source.clone(), Rc::new(compiled))
        });
        Ok(JqFilter { source })
    }

    /// Parse an optional jq filter (an empty string means no filter).
    pub fn parse_optional(code: &str) -> Result<Option<JqFilter>, String> {
        if code.is_empty() {
            Ok(None)
        } else {
            JqFilter::parse(code).map(Some)
        }
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    fn with_compiled<R>(&self, f: impl FnOnce(&CompiledFilter) -> R) -> Result<R, String> {
        let compiled = CACHE.with(|cache| -> Result<Rc<CompiledFilter>, String> {
            let mut cache = cache.borrow_mut();
            if let Some(filter) = cache.get(&self.source) {
                return Ok(filter.clone());
            }
            let filter = Rc::new(compile(&self.source)?);
            insert_in_cache(&mut cache, self.source.clone(), filter.clone());
            Ok(filter)
        })?;
        Ok(f(&compiled))
    }

    /// Run the filter and return its first output (`None` when the filter yields no output).
    pub fn first(&self, input: &Value) -> Result<Option<Value>, String> {
        self.with_compiled(|filter| {
            let inputs = RcIter::new(core::iter::empty());
            let mut outputs = filter.run((Ctx::new([], &inputs), Val::from(input.clone())));
            match outputs.next() {
                None => Ok(None),
                Some(Ok(v)) => Ok(Some(Value::from(v))),
                Some(Err(e)) => Err(e.to_string()),
            }
        })?
    }

    /// Run the filter and return all its outputs.
    pub fn all(&self, input: &Value) -> Result<Vec<Value>, String> {
        self.with_compiled(|filter| {
            let inputs = RcIter::new(core::iter::empty());
            filter
                .run((Ctx::new([], &inputs), Val::from(input.clone())))
                .map(|r| r.map(Value::from).map_err(|e| e.to_string()))
                .collect()
        })?
    }

    /// Return true when the first output of the filter is the boolean `true`.
    pub fn matches(&self, input: &Value) -> Result<bool, String> {
        Ok(matches!(self.first(input)?, Some(Value::Bool(true))))
    }
}

fn insert_in_cache(
    cache: &mut HashMap<Arc<str>, Rc<CompiledFilter>>,
    source: Arc<str>,
    filter: Rc<CompiledFilter>,
) {
    if cache.len() >= CACHE_CAPACITY {
        cache.clear();
    }
    cache.insert(source, filter);
}

fn compile(code: &str) -> Result<CompiledFilter, String> {
    let arena = Arena::default();
    let loader = Loader::new(jaq_std::defs().chain(jaq_json::defs()));
    let modules = loader
        .load(&arena, File { code, path: () })
        .map_err(|errors| {
            let details: Vec<String> = errors
                .into_iter()
                .map(|(_, error)| format_load_error(code, error))
                .collect();
            format!("invalid jq filter: {}", details.join("; "))
        })?;
    jaq_core::Compiler::default()
        .with_funs(jaq_std::funs().chain(jaq_json::funs()))
        .compile(modules)
        .map_err(|errors| {
            let details: Vec<String> = errors
                .into_iter()
                .flat_map(|(_, errs)| errs)
                .map(|(name, undefined)| format!("undefined {} {}", undefined.as_str(), name))
                .collect();
            format!("invalid jq filter: {}", details.join("; "))
        })
}

fn format_load_error(code: &str, error: jaq_core::load::Error<&str>) -> String {
    use jaq_core::load::Error;
    let position = |s: &str| {
        let offset = (s.as_ptr() as usize).saturating_sub(code.as_ptr() as usize);
        offset.min(code.len())
    };
    match error {
        Error::Io(errors) => errors
            .into_iter()
            .map(|(path, e)| format!("{path}: {e}"))
            .collect::<Vec<_>>()
            .join(", "),
        Error::Lex(errors) => errors
            .into_iter()
            .map(|(expect, found)| {
                format!(
                    "expected {} at position {}",
                    expect.as_str(),
                    position(found)
                )
            })
            .collect::<Vec<_>>()
            .join(", "),
        Error::Parse(errors) => errors
            .into_iter()
            .map(|(expect, found)| {
                if found.is_empty() {
                    format!(
                        "expected {} but reached the end of the filter",
                        expect.as_str()
                    )
                } else {
                    format!(
                        "expected {} at position {} but found \"{}\"",
                        expect.as_str(),
                        position(found),
                        found
                    )
                }
            })
            .collect::<Vec<_>>()
            .join(", "),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn first_output() {
        let f = JqFilter::parse(".m.name").unwrap();
        assert_eq!(
            f.first(&json!({"m": {"name": "x"}})).unwrap(),
            Some(json!("x"))
        );
        assert_eq!(f.first(&json!({"m": {}})).unwrap(), Some(Value::Null));
    }

    #[test]
    fn select_yields_no_output_when_not_matching() {
        let f = JqFilter::parse("select(.m.value > 10)").unwrap();
        assert_eq!(f.first(&json!({"m": {"value": 1}})).unwrap(), None);
        assert!(f.first(&json!({"m": {"value": 11}})).unwrap().is_some());
    }

    #[test]
    fn matches_requires_true() {
        let f = JqFilter::parse(r#".project == "demo" and .env == "test""#).unwrap();
        assert!(f
            .matches(&json!({"project": "demo", "env": "test"}))
            .unwrap());
        assert!(!f.matches(&json!({"project": "demo"})).unwrap());
        let f = JqFilter::parse(".name").unwrap();
        assert!(!f.matches(&json!({"name": "true"})).unwrap());
    }

    #[test]
    fn runtime_errors() {
        let f = JqFilter::parse(".a + 1").unwrap();
        assert!(f.first(&json!({"a": "text"})).is_err());
        assert_eq!(
            JqFilter::parse(".[]").unwrap().all(&json!([1, 2])).unwrap(),
            vec![json!(1), json!(2)]
        );
    }

    #[test]
    fn invalid_filters() {
        assert!(JqFilter::parse(".a ==").is_err());
        assert!(JqFilter::parse("unknownfunction(1)").is_err());
        assert!(JqFilter::parse("(").is_err());
        assert!(JqFilter::parse_optional("").unwrap().is_none());
    }

    #[test]
    fn usable_from_other_threads() {
        let f = JqFilter::parse(".x").unwrap();
        let handle = std::thread::spawn(move || f.first(&json!({"x": 1})).unwrap());
        assert_eq!(handle.join().unwrap(), Some(json!(1)));
    }
}
