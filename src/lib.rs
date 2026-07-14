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
/// Returns an `anyhow::Error` if initializing the system watcher fails
/// or if the watch service encounters a critical path access error.
///
/// # Examples
/// ```rust
/// use std::path::Path;
/// use watch_folder_lib::run_watch;
///
/// # fn main() -> anyhow::Result<()> {
/// let src = Path::new("src_folder");
/// let dst = Path::new("dst_folder");
///
/// // Simple example callback
/// let callback = |_src: &Path, _dst: &Path, event| {
///     println!("Detected event: {:?}", event);
///     Ok(())
/// };
///
/// // Note: the following line blocks the thread, so in tests/applications
/// // it is often run in a separate thread (std::thread::spawn).
/// // let _ = run_watch(src, dst, callback);
/// # Ok(())
/// # }
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::fs::File;
//     use std::sync::{Arc, Mutex};
//     use std::thread;
//     use std::time::Duration;
//     use tempfile::tempdir;

//     #[test]
//     fn test_run_watch_detects_file_creation() -> Result<()> {
//         // Create temporary directories which will be automatically removed when this function exits.
//         let tmp_src = tempdir()?;
//         let tmp_dst = tempdir()?;

//         let src_path = tmp_src.path().to_path_buf();
//         let dst_path = tmp_dst.path().to_path_buf();

//         // Prepare a shared structure to store information about detected events.
//         // We use `Arc` and `Mutex` because the callback will be invoked from another thread (the watcher thread).
//         let received_event_paths = Arc::new(Mutex::new(Vec::new()));
//         let cloned_paths = Arc::clone(&received_event_paths);

//         // Clone the paths for the thread that will run `run_watch`.
//         let src_path_thread = src_path.clone();
//         let dst_path_thread = dst_path.clone();

//         // Run `run_watch` in the background (in a separate thread) so the test is not blocked.
//         let _watcher_thread = thread::spawn(move || {
//             // Define our test callback function:
//             let test_callback = move |_src: &Path, _dst: &Path, event: Event| -> Result<()> {
//                 let mut paths = cloned_paths.lock().unwrap();
//                 // Record the file paths from the detected event
//                 for p in event.paths {
//                     paths.push(p);
//                 }
//                 Ok(())
//             };

//             let _ = run_watch(&src_path_thread, &dst_path_thread, test_callback);
//         });

//         // Give the background thread a short moment (e.g., 100ms) to initialize the watcher in the system.
//         thread::sleep(Duration::from_millis(100));

//         // Perform an action in the watched folder: create a file named "test_file.txt"
//         let test_file_path = src_path.join("test_file.txt");
//         let _file = File::create(&test_file_path)?;

//         // Operating systems need some time to report the event and deliver it to the mpsc channel.
//         // Wait up to 1 second for an entry to appear in our vector.
//         let mut attempts = 0;
//         while attempts < 10 {
//             thread::sleep(Duration::from_millis(100));
//             let paths = received_event_paths.lock().unwrap();
//             if !paths.is_empty() {
//                 break;
//             }
//             attempts += 1;
//         }

//         // Check the assertion: did our callback record the path of the newly created file?
//         let final_paths = received_event_paths.lock().unwrap();
//         assert!(!final_paths.is_empty(), "No event was detected!");
//         assert!(
//             final_paths.contains(&test_file_path),
//             "Detected paths {:?} do not contain the expected file {:?}",
//             final_paths,
//             test_file_path
//         );

//         Ok(())
//     }
// }
