use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
    io::ErrorKind,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IoError {
    FileRead { path: String, kind: ErrorKind },
    FileWrite { path: String, kind: ErrorKind },
    DirectoryCreation { path: String, kind: ErrorKind },
    CorruptedFile { path: String },
}

impl Error for IoError {}

impl Display for IoError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::FileRead { path, kind } => {
                write!(f, "Failed to read file `{path}`: {kind}")
            }
            Self::FileWrite { path, kind } => {
                write!(f, "Failed to write file `{path}`: {kind}")
            }
            Self::DirectoryCreation { path, kind } => {
                write!(f, "Failed to create directory `{path}`: {kind}")
            }
            Self::CorruptedFile { path } => {
                write!(f, "File `{path}` is corrupted")
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::IoError;
    use std::io::ErrorKind;

    #[test]
    fn test_display_files() {
        assert_eq!(
            "Failed to read file `path`: entity not found",
            IoError::FileRead {
                path: "path".to_string(),
                kind: ErrorKind::NotFound
            }
            .to_string()
        );
        assert_eq!(
            "Failed to write file `path`: permission denied",
            IoError::FileWrite {
                path: "path".to_string(),
                kind: ErrorKind::PermissionDenied
            }
            .to_string()
        );
        assert_eq!(
            "Failed to create directory `path`: permission denied",
            IoError::DirectoryCreation {
                path: "path".to_string(),
                kind: ErrorKind::PermissionDenied
            }
            .to_string()
        );
    }

    #[test]
    fn test_display_format() {
        assert_eq!(
            "File `path` is corrupted",
            IoError::CorruptedFile {
                path: "path".to_string()
            }
            .to_string()
        );
    }
}
