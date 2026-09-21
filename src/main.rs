use peisar_fs::MarkdownCache;
use std::io;
use std::thread;
use std::time::Duration;

fn main() -> io::Result<()> {
    // Read all markdown files into the in-memory cache
    let mut cache = MarkdownCache::new("contents")?;

    println!("Loaded {} markdown files", cache.all().len());
    for (path, text) in cache.all() {
        println!("{} ({} bytes)", path.display(), text.len());
    }

    // Start watching the directory for changes. The cache will be kept up-to-date.
    cache.start_watching()?;
    println!("Watching 'contents' for changes. Press Ctrl+C to exit.");

    // Keep the program alive so the watcher runs. In a real application you would
    // integrate this into your runtime or gracefully shut down on signal.
    loop {
        thread::sleep(Duration::from_secs(180));
        println!("Cache has {} files", cache.all().len());
    }
}
