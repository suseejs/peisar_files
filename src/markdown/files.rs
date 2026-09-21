use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// https://github.com/sindresorhus/markdown-extensions
pub const MARKDOWN_EXTENSIONS: &'static [&str] = &[
    "md", "markdown", "mdown", "mkdn", "mkd", "mdwn", "mkdown", "ron",
];

pub fn collect_markdown_files<P: AsRef<Path>>(entry_dir: P) -> io::Result<Vec<PathBuf>> {
    let current_dir = env::current_dir()?;
    let dir_path = current_dir.join(entry_dir.as_ref());
    let mut markdown_files: Vec<PathBuf> = Vec::new();

    if dir_path.is_dir() {
        for entry in fs::read_dir(&dir_path)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                // Recurse into subdirectory; PathBuf implements AsRef<Path>
                let nested = collect_markdown_files(path)?;
                markdown_files.extend(nested);
            } else if path.is_file() {
                if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                    if MARKDOWN_EXTENSIONS.contains(&ext) {
                        markdown_files.push(path);
                    }
                }
            }
        }
    }

    Ok(markdown_files)
}
