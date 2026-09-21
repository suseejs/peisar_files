use super::files::{MARKDOWN_EXTENSIONS, collect_markdown_files};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use serde_yaml::Value as YamlValue;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, mpsc};
use std::thread;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct CacheEntry {
    file_path: String,
    markdown_raw_content: String,
    frontmatter_data: Option<YamlValue>,
}

fn parse_frontmatter(content: &str) -> Option<YamlValue> {
    let mut lines = content.lines();
    if let Some(first) = lines.next() {
        if first.trim() == "---" {
            let mut fm_lines: Vec<&str> = Vec::new();
            for line in lines {
                if line.trim() == "---" {
                    break;
                }
                fm_lines.push(line);
            }
            let fm_str = fm_lines.join("\n");
            match serde_yaml::from_str(&fm_str) {
                Ok(v) => Some(v),
                Err(_) => None,
            }
        } else {
            None
        }
    } else {
        None
    }
}

#[derive(Debug)]
enum PersistCommand {
    Update(PathBuf, String),
    Remove(PathBuf),
    SyncAll(HashMap<PathBuf, String>),
}

fn compute_target_for_source(src: &Path, cwd: &Path) -> (PathBuf, PathBuf) {
    // returns (target_path, rel_path)
    let rel_path = match src.strip_prefix(cwd) {
        Ok(rel) => rel.to_path_buf(),
        Err(_) => src.to_path_buf(),
    };
    let parent = rel_path.parent().map(|p| p.to_path_buf());
    let fname = rel_path.file_name().map(|f| f.to_string_lossy().to_string()).unwrap_or_default();
    let json_name = format!("{}.json", fname);
    let cache_dir = cwd.join(".peisar_cache");
    let target_dir = match &parent {
        Some(p) => cache_dir.join(p),
        None => cache_dir.clone(),
    };
    let target = target_dir.join(&json_name);
    (target, rel_path)
}

fn persist_cache_map(map: &HashMap<PathBuf, String>) -> io::Result<()> {
    let cwd = env::current_dir()?;
    let cache_dir = cwd.join(".peisar_cache");
    fs::create_dir_all(&cache_dir)?;

    let mut expected_files: HashSet<PathBuf> = HashSet::new();

    for (path, content) in map {
        let (target, rel_path) = compute_target_for_source(path, &cwd);
        let target_dir = target.parent().map(|p| p.to_path_buf()).unwrap_or(cache_dir.clone());
        fs::create_dir_all(&target_dir)?;

        let file_path_str = rel_path.to_string_lossy().to_string();
        let fm = parse_frontmatter(content);
        let entry = CacheEntry {
            file_path: file_path_str,
            markdown_raw_content: content.clone(),
            frontmatter_data: fm,
        };

        let json = serde_json::to_string_pretty(&entry)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("serde_json error: {}", e)))?;

        // Write atomically to a temp file then rename
        let fname = target.file_name().map(|f| f.to_string_lossy().to_string()).unwrap_or_default();
        let tmp_name = format!("{}.tmp", fname);
        let tmp_path = target_dir.join(&tmp_name);
        fs::write(&tmp_path, json.as_bytes())?;
        fs::rename(&tmp_path, &target)?;

        expected_files.insert(target);
    }

    // Remove stale files that are present in .peisar_cache but not expected
    fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) -> io::Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let p = entry.path();
            if p.is_dir() {
                collect_files(&p, out)?;
            } else if p.is_file() {
                out.push(p);
            }
        }
        Ok(())
    }

    if cache_dir.exists() {
        let mut existing_files: Vec<PathBuf> = Vec::new();
        collect_files(&cache_dir, &mut existing_files)?;
        for f in existing_files {
            if !expected_files.contains(&f) {
                let _ = fs::remove_file(&f);
            }
        }

        // Remove empty directories under .peisar_cache
        fn remove_empty(dir: &Path) -> io::Result<()> {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let p = entry.path();
                if p.is_dir() {
                    remove_empty(&p)?;
                    if fs::read_dir(&p)?.next().is_none() {
                        let _ = fs::remove_dir(&p);
                    }
                }
            }
            Ok(())
        }
        let _ = remove_empty(&cache_dir);
    }

    // Ensure .peisar_cache is in .gitignore
    let gitignore_path = cwd.join(".gitignore");
    if gitignore_path.exists() {
        let existing = fs::read_to_string(&gitignore_path)?;
        let mut has_entry = false;
        for line in existing.lines() {
            if line.trim() == ".peisar_cache" {
                has_entry = true;
                break;
            }
        }
        if !has_entry {
            let mut file = OpenOptions::new().append(true).open(&gitignore_path)?;
            if !existing.ends_with('\n') {
                file.write_all(b"\n")?;
            }
            file.write_all(b".peisar_cache\n")?;
        }
    } else {
        fs::write(gitignore_path, ".peisar_cache\n")?;
    }

    Ok(())
}

/// In-memory cache of markdown files under a given directory.
/// The cache maps absolute PathBuf -> raw file contents.
pub struct MarkdownCache {
    cache: Arc<RwLock<HashMap<PathBuf, String>>>,
    watcher: Option<RecommendedWatcher>,
    entry_dir: PathBuf,
    // Sender for persistence commands — None until worker is started in new()
    persist_tx: Option<mpsc::Sender<PersistCommand>>,
    // Background worker handle to join on drop
    worker_handle: Option<thread::JoinHandle<()>>,
}

impl MarkdownCache {
    /// Load all markdown files under `entry_dir` into the cache.
    /// Also persists the cache to the `.peisar_cache` directory and ensures
    /// the `.gitignore` contains an entry for `.peisar_cache`.
    pub fn new<P: AsRef<Path>>(entry_dir: P) -> io::Result<Self> {
        let entry_dir = entry_dir.as_ref().to_path_buf();
        let mut cache_map: HashMap<PathBuf, String> = HashMap::new();

        let files = collect_markdown_files(&entry_dir)?;
        for f in files {
            if let Ok(text) = fs::read_to_string(&f) {
                cache_map.insert(f, text);
            }
        }

        // Persist the initial cache to disk synchronously so callers observe
        // cache files immediately after construction.
        persist_cache_map(&cache_map)?;

        // Prepare background persistence worker that accepts incremental
        // commands so the watcher callback can remain non-blocking for large
        // repositories. Seed the worker's hash map with the current contents
        // to enable content-hash-based de-duplication.
        let cwd = env::current_dir()?;
        let (tx, rx) = mpsc::channel::<PersistCommand>();

        let mut last_hashes: HashMap<PathBuf, String> = HashMap::new();
        for (src, content) in &cache_map {
            let (target, _rel) = compute_target_for_source(src, &cwd);
            let hash = blake3::hash(content.as_bytes()).to_hex().to_string();
            last_hashes.insert(target, hash);
        }

        let handle = thread::spawn(move || {
            let cache_dir = cwd.join(".peisar_cache");

            // helper to collect all files under cache_dir
            fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) -> io::Result<()> {
                for entry in fs::read_dir(dir)? {
                    let entry = entry?;
                    let p = entry.path();
                    if p.is_dir() {
                        collect_files(&p, out)?;
                    } else if p.is_file() {
                        out.push(p);
                    }
                }
                Ok(())
            }

            let mut hashes = last_hashes;

            while let Ok(cmd) = rx.recv() {
                match cmd {
                    PersistCommand::Update(src, content) => {
                        let (target, rel_path) = compute_target_for_source(&src, &cwd);
                        let target_dir = target.parent().map(|p| p.to_path_buf()).unwrap_or(cache_dir.clone());
                        if let Err(e) = fs::create_dir_all(&target_dir) {
                            eprintln!("persist mkdir error: {:?}", e);
                            continue;
                        }
                        let hash = blake3::hash(content.as_bytes()).to_hex().to_string();
                        if let Some(prev) = hashes.get(&target) {
                            if prev == &hash {
                                // nothing to do
                                continue;
                            }
                        }

                        let fm = parse_frontmatter(&content);
                        let entry = CacheEntry {
                            file_path: rel_path.to_string_lossy().to_string(),
                            markdown_raw_content: content.clone(),
                            frontmatter_data: fm,
                        };

                        match serde_json::to_string_pretty(&entry) {
                            Ok(json) => {
                                let fname = target.file_name().map(|f| f.to_string_lossy().to_string()).unwrap_or_default();
                                let tmp_name = format!("{}.tmp", fname);
                                let tmp_path = target_dir.join(&tmp_name);
                                if let Err(e) = fs::write(&tmp_path, json.as_bytes()) {
                                    eprintln!("persist write tmp error: {:?}", e);
                                    continue;
                                }
                                if let Err(e) = fs::rename(&tmp_path, &target) {
                                    eprintln!("persist rename error: {:?}", e);
                                    let _ = fs::remove_file(&tmp_path);
                                    continue;
                                }
                                hashes.insert(target, hash);
                            }
                            Err(e) => eprintln!("serde_json error: {:?}", e),
                        }
                    }
                    PersistCommand::Remove(src) => {
                        let (target, _rel) = compute_target_for_source(&src, &cwd);
                        if target.exists() {
                            if let Err(e) = fs::remove_file(&target) {
                                eprintln!("persist remove error: {:?}", e);
                            }
                        }
                        hashes.remove(&target);

                        // clean up empty parent directories
                        if let Some(mut parent) = target.parent().map(|p| p.to_path_buf()) {
                            while parent.starts_with(&cache_dir) && fs::read_dir(&parent).map(|mut it| it.next().is_none()).unwrap_or(false) {
                                let _ = fs::remove_dir(&parent);
                                if let Some(p) = parent.parent() {
                                    parent = p.to_path_buf();
                                } else {
                                    break;
                                }
                            }
                        }
                    }
                    PersistCommand::SyncAll(map) => {
                        let mut expected: HashSet<PathBuf> = HashSet::new();
                        for (src, content) in map {
                            let (target, rel_path) = compute_target_for_source(&src, &cwd);
                            expected.insert(target.clone());

                            let target_dir = target.parent().map(|p| p.to_path_buf()).unwrap_or(cache_dir.clone());
                            if let Err(e) = fs::create_dir_all(&target_dir) {
                                eprintln!("persist mkdir error: {:?}", e);
                                continue;
                            }

                            let hash = blake3::hash(content.as_bytes()).to_hex().to_string();
                            if let Some(prev) = hashes.get(&target) {
                                if prev == &hash {
                                    continue;
                                }
                            }

                            let fm = parse_frontmatter(&content);
                            let entry = CacheEntry {
                                file_path: rel_path.to_string_lossy().to_string(),
                                markdown_raw_content: content.clone(),
                                frontmatter_data: fm,
                            };

                            match serde_json::to_string_pretty(&entry) {
                                Ok(json) => {
                                    let fname = target.file_name().map(|f| f.to_string_lossy().to_string()).unwrap_or_default();
                                    let tmp_name = format!("{}.tmp", fname);
                                    let tmp_path = target_dir.join(&tmp_name);
                                    if let Err(e) = fs::write(&tmp_path, json.as_bytes()) {
                                        eprintln!("persist write tmp error: {:?}", e);
                                        continue;
                                    }
                                    if let Err(e) = fs::rename(&tmp_path, &target) {
                                        eprintln!("persist rename error: {:?}", e);
                                        let _ = fs::remove_file(&tmp_path);
                                        continue;
                                    }
                                    hashes.insert(target, hash);
                                }
                                Err(e) => eprintln!("serde_json error: {:?}", e),
                            }
                        }

                        // remove stale
                        if cache_dir.exists() {
                            let mut existing: Vec<PathBuf> = Vec::new();
                            if let Err(e) = collect_files(&cache_dir, &mut existing) {
                                eprintln!("collect files error: {:?}", e);
                            } else {
                                for f in existing {
                                    if !expected.contains(&f) {
                                        let _ = fs::remove_file(&f);
                                        hashes.remove(&f);
                                    }
                                }
                            }

                            // remove empty dirs
                            fn remove_empty(dir: &Path) -> io::Result<()> {
                                for entry in fs::read_dir(dir)? {
                                    let entry = entry?;
                                    let p = entry.path();
                                    if p.is_dir() {
                                        remove_empty(&p)?;
                                        if fs::read_dir(&p)?.next().is_none() {
                                            let _ = fs::remove_dir(&p);
                                        }
                                    }
                                }
                                Ok(())
                            }
                            let _ = remove_empty(&cache_dir);
                        }
                    }
                }
            }
        });

        Ok(MarkdownCache {
            cache: Arc::new(RwLock::new(cache_map)),
            watcher: None,
            entry_dir,
            persist_tx: Some(tx),
            worker_handle: Some(handle),
        })
    }

    /// Start watching the entry_dir recursively. The watcher will update the
    /// in-memory cache on create/modify/remove events for markdown files and
    /// persist updates to disk.
    ///
    /// The returned Result is only for watcher setup errors; runtime errors are
    /// printed to stderr by the watch callback.
    pub fn start_watching(&mut self) -> io::Result<()> {
        let cache = Arc::clone(&self.cache);
        let entry_dir = self.entry_dir.clone();

        let tx = match &self.persist_tx {
            Some(t) => t.clone(),
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "persistence worker not started",
                ))
            }
        };

        // recommended_watcher takes a closure that's invoked on file events.
        let mut watcher: RecommendedWatcher =
            notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
                match res {
                    Ok(event) => {
                        for path in event.paths {
                            // If the event targets a directory, rescan everything.
                            if path.is_dir() {
                                if let Ok(files) = collect_markdown_files(&entry_dir) {
                                    let mut new_map = HashMap::new();
                                    for f in files {
                                        if let Ok(text) = fs::read_to_string(&f) {
                                            new_map.insert(f, text);
                                        }
                                    }
                                    if let Ok(mut w) = cache.write() {
                                        *w = new_map.clone();
                                        if let Err(e) = tx.send(PersistCommand::SyncAll(new_map)) {
                                            eprintln!("persist send error: {:?}", e);
                                        }
                                    }
                                }
                                continue;
                            }

                            if path.is_file() {
                                if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                                    if MARKDOWN_EXTENSIONS.contains(&ext) {
                                        match fs::read_to_string(&path) {
                                            Ok(text) => {
                                                if let Ok(mut w) = cache.write() {
                                                    w.insert(path.clone(), text.clone());
                                                    if let Err(e) = tx.send(PersistCommand::Update(path.clone(), text)) {
                                                        eprintln!("persist send error: {:?}", e);
                                                    }
                                                }
                                            }
                                            Err(_) => {
                                                if let Ok(mut w) = cache.write() {
                                                    w.remove(&path);
                                                    if let Err(e) = tx.send(PersistCommand::Remove(path.clone())) {
                                                        eprintln!("persist send error: {:?}", e);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => eprintln!("watch error: {:?}", e),
                }
            })
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("notify::recommended_watcher error: {}", e),
                )
            })?;

        watcher
            .watch(&self.entry_dir, RecursiveMode::Recursive)
            .map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("notify::watch error: {}", e))
            })?;
        self.watcher = Some(watcher);
        Ok(())
    }

    /// Get a cached markdown file's raw contents by path, if present.
    pub fn get(&self, path: &Path) -> Option<String> {
        match self.cache.read() {
            Ok(r) => r.get(path).cloned(),
            Err(_) => None,
        }
    }

    /// Return a clone of the entire cache map.
    pub fn all(&self) -> HashMap<PathBuf, String> {
        match self.cache.read() {
            Ok(r) => r.clone(),
            Err(_) => HashMap::new(),
        }
    }
}

impl Drop for MarkdownCache {
    fn drop(&mut self) {
        // Closing the sender will cause the worker thread to exit.
        self.persist_tx.take();
        if let Some(handle) = self.worker_handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use std::io::Write;
    use std::sync::Mutex;

    static TEST_CWD_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn test_persist_cache_and_gitignore_created() -> io::Result<()> {
        let _guard = TEST_CWD_LOCK.lock().unwrap();
        let orig = env::current_dir()?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("time error: {}", e)))?
            .as_millis();
        let tmp = env::temp_dir().join(format!("peisar_test_{}", now));
        if tmp.exists() {
            fs::remove_dir_all(&tmp)?;
        }
        fs::create_dir_all(&tmp)?;
        env::set_current_dir(&tmp)?;

        fs::create_dir_all("docs")?;
        let md_path = tmp.join("docs").join("doc1.md");
        let mut f = File::create(&md_path)?;
        f.write_all(b"---\ntitle: Test\ntags:\n  - a\n  - b\n---\n# Hello\n")?;
        f.sync_all()?;

        let _cache = MarkdownCache::new("docs")?;

        let cache_file = tmp.join(".peisar_cache").join("docs").join("doc1.md.json");
        assert!(cache_file.exists());
        let s = fs::read_to_string(&cache_file)?;
        let entry: CacheEntry = serde_json::from_str(&s).unwrap();
        assert!(entry.file_path.ends_with("docs/doc1.md") || entry.file_path.ends_with("docs\\doc1.md"));
        assert!(entry.frontmatter_data.is_some());

        let gitignore = tmp.join(".gitignore");
        assert!(gitignore.exists());
        let git = fs::read_to_string(&gitignore)?;
        assert!(git.lines().any(|l| l.trim() == ".peisar_cache"));

        env::set_current_dir(orig)?;
        fs::remove_dir_all(&tmp)?;
        Ok(())
    }

    #[test]
    fn test_gitignore_not_duplicated() -> io::Result<()> {
        let _guard = TEST_CWD_LOCK.lock().unwrap();
        let orig = env::current_dir()?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("time error: {}", e)))?
            .as_millis();
        let tmp = env::temp_dir().join(format!("peisar_test_{}", now));
        if tmp.exists() {
            fs::remove_dir_all(&tmp)?;
        }
        fs::create_dir_all(&tmp)?;
        env::set_current_dir(&tmp)?;

        // pre-create .gitignore with entry
        fs::write(".gitignore", ".peisar_cache\n")?;

        fs::create_dir_all("docs")?;
        let md_path = tmp.join("docs").join("doc2.md");
        let mut f = File::create(&md_path)?;
        f.write_all(b"# No frontmatter\nContent\n")?;
        f.sync_all()?;

        let _cache = MarkdownCache::new("docs")?;

        let git = fs::read_to_string(".gitignore")?;
        let occurrences = git.lines().filter(|l| l.trim() == ".peisar_cache").count();
        assert_eq!(occurrences, 1);

        env::set_current_dir(orig)?;
        fs::remove_dir_all(&tmp)?;
        Ok(())
    }

    #[test]
    fn test_stale_cache_removal_on_remove_command() -> io::Result<()> {
        let _guard = TEST_CWD_LOCK.lock().unwrap();
        let orig = env::current_dir()?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("time error: {}", e)))?
            .as_millis();
        let tmp = env::temp_dir().join(format!("peisar_test_{}", now));
        if tmp.exists() {
            fs::remove_dir_all(&tmp)?;
        }
        fs::create_dir_all(&tmp)?;
        env::set_current_dir(&tmp)?;

        fs::create_dir_all("docs")?;
        let md1 = tmp.join("docs").join("a.md");
        let md2 = tmp.join("docs").join("b.md");
        {
            let mut f1 = File::create(&md1)?;
            f1.write_all(b"# A\n")?;
            f1.sync_all()?;
            let mut f2 = File::create(&md2)?;
            f2.write_all(b"# B\n")?;
            f2.sync_all()?;
        }

        let cache = MarkdownCache::new("docs")?;

        let cache_a = tmp.join(".peisar_cache").join("docs").join("a.md.json");
        let cache_b = tmp.join(".peisar_cache").join("docs").join("b.md.json");
        assert!(cache_a.exists());
        assert!(cache_b.exists());

        // Remove source file and send Remove command to worker
        fs::remove_file(&md1)?;
        if let Some(tx) = &cache.persist_tx {
            tx.send(PersistCommand::Remove(md1.clone())).unwrap();
        }

        // Wait for worker to process
        let mut waited = 0u32;
        while cache_a.exists() && waited < 50 {
            std::thread::sleep(std::time::Duration::from_millis(20));
            waited += 1;
        }

        assert!(!cache_a.exists());

        env::set_current_dir(orig)?;
        fs::remove_dir_all(&tmp)?;
        Ok(())
    }

    #[test]
    fn test_hashing_avoids_rewrite() -> io::Result<()> {
        let _guard = TEST_CWD_LOCK.lock().unwrap();
        let orig = env::current_dir()?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("time error: {}", e)))?
            .as_millis();
        let tmp = env::temp_dir().join(format!("peisar_test_{}", now));
        if tmp.exists() {
            fs::remove_dir_all(&tmp)?;
        }
        fs::create_dir_all(&tmp)?;
        env::set_current_dir(&tmp)?;

        fs::create_dir_all("docs")?;
        let md = tmp.join("docs").join("doc.md");
        let content = b"# Title\nContent\n";
        {
            let mut f = File::create(&md)?;
            f.write_all(content)?;
            f.sync_all()?;
        }

        let cache = MarkdownCache::new("docs")?;
        let cache_file = tmp.join(".peisar_cache").join("docs").join("doc.md.json");
        assert!(cache_file.exists());

        let meta1 = fs::metadata(&cache_file)?.modified()?;
        // send update with same content
        if let Some(tx) = &cache.persist_tx {
            tx.send(PersistCommand::Update(md.clone(), String::from_utf8_lossy(content).to_string())).unwrap();
        }

        // Wait briefly to let worker run
        std::thread::sleep(std::time::Duration::from_millis(200));
        let meta2 = fs::metadata(&cache_file)?.modified()?;

        assert_eq!(meta1, meta2, "file was rewritten despite identical content");

        env::set_current_dir(orig)?;
        fs::remove_dir_all(&tmp)?;
        Ok(())
    }
}
