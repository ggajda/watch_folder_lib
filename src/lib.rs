//! # Watch Folder Lib
//!
//! A library for continuously monitoring changes in a source directory
//! and processing them asynchronously using a Tokio task queue.
//!
use anyhow::Result;
use log::{error, info};
use notify::{Event, RecursiveMode, Watcher};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::mpsc as std_mpsc; // Synchronous channel for notify
use tokio::sync::mpsc as tk_mpsc; // Asynchronous channel for the Tokio queue

/// Structure representing a single job in the queue.
#[derive(Debug)]
pub struct Job {
    pub event: Event,
    pub dst_path: PathBuf,
}

/// Asynchronous worker service that reads jobs from the queue
/// and executes the user-provided callback for each job.
async fn mpsc_service<F, Fut>(mut rx: tk_mpsc::Receiver<Job>, callback_fn: F)
where
    F: Fn(PathBuf, Event) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    info!("[Queue] The task queue service has started.");

    while let Some(job) = rx.recv().await {
        let dst = job.dst_path;
        let event = job.event;

        // Invoke the user's asynchronous callback for each job
        if let Err(e) = callback_fn(dst, event).await {
            error!(
                "[Queue] An error occurred while executing the callback for a job: {:?}",
                e
            );
        }
    }

    info!("[Queue] The task queue has been closed.");
}

/// Main function that starts the whole system.
///
/// 1. Creates an asynchronous task queue.
/// 2. Starts an asynchronous worker in the background.
/// 3. Blocks the current thread while monitoring files and pushing them into the queue.
pub fn run_watch<F, Fut>(src_path: &Path, dst_path: &Path, callback_fn: F) -> Result<()>
where
    F: Fn(PathBuf, Event) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    // 1. Create an asynchronous Tokio channel (task queue with capacity 100)
    let (tx, rx) = tk_mpsc::channel::<Job>(100);

    // 2. Start mpsc_service in the background on the Tokio runtime
    tokio::spawn(mpsc_service(rx, callback_fn));

    // 3. Create a synchronous channel for the notify library
    let (std_tx, std_rx) = std_mpsc::channel::<Result<Event, notify::Error>>();

    // 4. Initialize the system file watcher
    let mut watcher = notify::recommended_watcher(std_tx)?;
    watcher.watch(src_path, RecursiveMode::NonRecursive)?;

    info!("[Watcher] Started monitoring folder: {:?}", src_path);

    // 5. Blocking loop that receives events from disk and pushes them into the Tokio queue
    for res in std_rx {
        match res {
            Ok(event) => {
                let job = Job {
                    event,
                    dst_path: dst_path.to_path_buf(),
                };

                // blocking_send() allows the synchronous thread
                // to safely enqueue a job into the asynchronous queue
                if let Err(_) = tx.blocking_send(job) {
                    error!("[Watcher] Failed to add a job. The queue is closed.");
                    break;
                }
            }
            Err(e) => error!("[Watcher] System notification error: {:?}", e),
        }
    }

    Ok(())
}
