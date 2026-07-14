//! # Watch Folder Lib
//!
//! A library for continuously monitoring changes in a source directory.
//! It uses the `notify` crate internally to provide native and efficient
//! filesystem event notifications (create, modify, delete events).
//!
//! ## Main features
//! - Automatically selects the best watcher implementation for the current platform (Inotify, Fsevent, ReadDirectoryChangesW).
//! - Forwards events to custom callback functions.
//! - Error handling support via `anyhow`.
//!
use ::log::{error, info};
use anyhow::Result;
use notify::{Event, RecursiveMode, Watcher};
use std::{path::Path, sync::mpsc};

/// Starts a watch service for the given source path.
///
/// This function **blocks the current thread indefinitely** while listening for filesystem events.
/// When a change is detected, it forwards the event data to the provided `watch_fn` callback.
///
/// # Arguments
/// * `src_path` - The path to the directory that should be watched.
/// * `dst_path` - The destination path (can be used inside the callback, e.g. for synchronization).
/// * `callback_fn` - The callback function invoked for each detected event.
///
/// # Errors
/// Returns an `notify::Error` if initializing the system watcher fails
/// or if the watch service encounters a critical path access error.
///
/// # Examples
///
/// ```no_run
/// use anyhow::Result;
/// use notify::Event;
/// use std::path::Path;
/// use watch_folder_lib::run_watch;
///
/// fn main() -> Result<()> {
///     let src = Path::new("src_folder");
///     let dst = Path::new("dst_folder");
///
///     let callback = |_src: &Path,
///                     _dst: &Path,
///                     event: Event|
///      -> Result<()> {
///         println!("Detected event: {:?}", event);
///         Ok(())
///     };
///
///     run_watch(src, dst, callback)?;
///
///     Ok(())
/// }
/// ```

pub fn run_watch<F>(src_path: &Path, dst_path: &Path, callback_fn: F) -> Result<()>
where
    F: Fn(&Path, &Path, Event) -> Result<()>,
{
    let (tx, rx) = mpsc::channel::<Result<Event, notify::Error>>();

    // Use recommended_watcher() to automatically select the best implementation
    // for your platform. The `EventHandler` passed to this constructor can be a
    // closure, a `std::sync::mpsc::Sender`, a `crossbeam_channel::Sender`, or
    // another type the trait is implemented for.
    let mut watcher = notify::recommended_watcher(tx)?;

    info!("Watch service is running...");

    // Add a path to be watched. All files and directories at that path and
    // below will be monitored for changes.
    watcher.watch(src_path, RecursiveMode::Recursive)?;
    // Block forever, printing out events as they come in
    for res in rx {
        match res {
            Ok(event) => callback_fn(src_path, dst_path, event)?,
            //Ok(event) => info!("WATCH EVENT: {:?}", event),
            Err(e) => error!("watch error: {:?}", e),
        }
    }

    Ok(())
}
