//! # Watch Folder Lib
//!
//! `watch_folder_lib` is a lightweight asynchronous library for monitoring
//! filesystem changes in a directory and processing detected events concurrently.
//!
//! The library is built on top of the `notify` crate and automatically selects
//! the most appropriate native filesystem watcher for the current operating
//! system.
//!
//! ## Features
//!
//! - Watches a source directory for filesystem changes.
//! - Supports create, modify, remove and rename events.
//! - Processes events asynchronously using Tokio.
//! - Limits the number of concurrently running callback tasks.
//! - Cross-platform support (Windows, Linux and macOS).
//!
//! ## How it works
//!
//! 1. The library starts monitoring the specified source directory.
//! 2. Every filesystem event is received from `notify`.
//! 3. Each event is processed in its own Tokio task.
//! 4. A semaphore limits how many callbacks may execute simultaneously.
//!
//! ```text
//!        notify
//!           │
//!           ▼
//!     filesystem event
//!           │
//!           ▼
//!      Tokio task
//!           │
//!           ▼
//!      user callback
//! ```
//!
//! ## Callback
//!
//! The callback receives:
//!
//! - source directory (`PathBuf`)
//! - destination directory (`PathBuf`)
//! - detected `notify::Event`
//!
//! The callback is asynchronous, allowing long-running operations such as:
//!
//! - copying files,
//! - image processing,
//! - compression,
//! - synchronization,
//! - uploading files.
//!
//! ## Example
//!
//! ```no_run
//! use anyhow::Result;
//! use notify::Event;
//! use std::path::PathBuf;
//! use watch_folder_lib::run_watch;
//!
//! async fn callback(
//!     src: PathBuf,
//!     dst: PathBuf,
//!     event: Event,
//! ) -> Result<()> {
//!     println!("{:?}", event);
//!
//!     // Process files here...
//!
//!     Ok(())
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     run_watch(
//!         std::path::Path::new("./source"),
//!         std::path::Path::new("./destination"),
//!         callback,
//!     )
//!     .await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Notes
//!
//! - The function runs continuously until the application exits.
//! - The callback is executed in a separate Tokio task.
//! - The maximum number of concurrently running callbacks is limited by an
//!   internal semaphore.
//! - The callback should return quickly or perform long operations
//!   asynchronously.
//!
/// Starts asynchronous monitoring of a directory.
///
/// The function watches `src_path` for filesystem changes and invokes the
/// supplied asynchronous callback for every detected event.
///
/// Every callback execution runs in its own Tokio task.
///
/// # Arguments
///
/// * `src_path` - Directory to monitor.
/// * `dst_path` - Destination directory passed to the callback.
/// * `callback_fn` - Asynchronous callback executed for every filesystem event.
///
/// The callback receives:
///
/// * `PathBuf` - Source directory.
/// * `PathBuf` - Destination directory.
/// * `notify::Event` - Filesystem event.
///
/// # Concurrency
///
/// Callback execution is limited by an internal semaphore to prevent creating
/// an unlimited number of simultaneously running tasks.
///
/// # Errors
///
/// Returns an error if:
///
/// - the watcher cannot be initialized,
/// - one of the directories cannot be created,
/// - the underlying filesystem watcher reports a fatal error.
///
/// # Example
///
/// ```no_run
/// use anyhow::Result;
/// use notify::Event;
/// use std::path::PathBuf;
///
/// async fn callback(
///     src: PathBuf,
///     dst: PathBuf,
///     event: Event,
/// ) -> Result<()> {
///     println!("{:?}", event);
///     Ok(())
/// }
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// watch_folder_lib::run_watch(
///     std::path::Path::new("./src"),
///     std::path::Path::new("./dst"),
///     callback,
/// )
/// .await?;
/// # Ok(())
/// # }
/// ```
use anyhow::Result;
use log::{error, info};
use notify::{Event, RecursiveMode, Watcher};
use std::{fs::create_dir_all, path::Path, path::PathBuf, sync::Arc, sync::mpsc};
use tokio::sync::Semaphore;

pub async fn run_watch<F, Fut>(src_path: &Path, dst_path: &Path, callback_fn: F) -> Result<()>
where
    F: Fn(PathBuf, PathBuf, Event) -> Fut + Send + Sync + Clone + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    create_dir_all(src_path)?;
    create_dir_all(dst_path)?;

    let src_path = src_path.to_path_buf();
    let dst_path = dst_path.to_path_buf();

    let (tx, rx) = mpsc::channel();

    let mut watcher = notify::recommended_watcher(tx)?;

    info!("Watch service is running...");

    watcher.watch(&src_path, RecursiveMode::NonRecursive)?;

    let semaphore = Arc::new(Semaphore::new(4));

    for res in rx {
        match res {
            Ok(event) => {
                let callback = callback_fn.clone();
                let src = src_path.clone();
                let dst = dst_path.clone();
                let sem = semaphore.clone();

                tokio::spawn(async move {
                    let _permit = sem.acquire_owned().await.unwrap();

                    if let Err(e) = callback(src, dst, event).await {
                        log::error!("callback error: {e}");
                    }
                });
            }
            Err(e) => error!("watch error: {:?}", e),
        }
    }

    Ok(())
}
