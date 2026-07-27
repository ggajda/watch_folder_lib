use anyhow::Result;
use log::{error, info};
use notify::{Event, RecursiveMode, Watcher};
use std::{fs::create_dir_all, path::Path, path::PathBuf, sync::Arc, sync::mpsc};
use tokio::sync::Semaphore;

pub fn run_watch<F, Fut>(src_path: &Path, dst_path: &Path, callback_fn: F) -> Result<()>
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
