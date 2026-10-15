//! Crate wide error type.

/// Errors raised by the internals of ministream (storage, streams, auth, ...).
///
/// HTTP facing errors are represented by [`crate::apierror::ApiError`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Generic error with a human readable message.
    #[error("{0}")]
    Msg(String),

    /// A requested resource (stream, iterator, message, user, ...) does not exist.
    #[error("{0}")]
    NotFound(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[cfg(feature = "mysql")]
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
}

impl Error {
    pub fn msg(message: impl Into<String>) -> Self {
        Error::Msg(message.into())
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Error::NotFound(message.into())
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Build an [`Error::Msg`] from a format string.
#[macro_export]
macro_rules! err {
    ($($arg:tt)*) => {
        $crate::error::Error::Msg(format!($($arg)*))
    };
}
