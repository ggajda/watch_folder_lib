#[cfg(test)]
mod tests {
    use anyhow::Result;
    use notify::event::Event;
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::sync::Notify;
    use watch_folder_lib::run_watch;

    #[tokio::test]
    async fn test_run_watch_captures_file_creation() -> Result<()> {
        // 1. Create temporary folders for the test
        let tmp_dir = tempdir()?;
        let src_dir = tmp_dir.path().join("src");
        let dst_dir = tmp_dir.path().join("dst");

        std::fs::create_dir(&src_dir)?;
        std::fs::create_dir(&dst_dir)?;

        // 2. Synchronization tool for the test thread.
        // We will use `tokio::sync::Notify` so the callback can signal
        // the main test thread that the job was processed successfully.
        let notify = Arc::new(Notify::new());
        let notify_clone = Arc::clone(&notify);

        // 3. Define an asynchronous test callback
        let callback = move |dst_path: PathBuf, event: Event| {
            let notify_trigger = Arc::clone(&notify_clone);
            async move {
                println!(
                    "[Test Callback] Received event in directory: {:?}",
                    dst_path
                );

                // Check whether the event corresponds to file creation
                if event.kind.is_create() {
                    println!("[Test Callback] Success: file creation detected!");
                    // Notify the main test thread that the test succeeded
                    notify_trigger.notify_one();
                }

                Ok(())
            }
        };

        // 4. Start run_watch in the background (spawn_blocking), since it blocks the thread
        let src_dir_clone = src_dir.clone();
        let dst_dir_clone = dst_dir.clone();

        let watcher_handle = tokio::task::spawn_blocking(move || {
            run_watch(&src_dir_clone, &dst_dir_clone, callback)
        });

        // Give the operating system a fraction of a second to start the watcher
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // 5. Trigger a real disk event by creating a new file in the monitored directory
        let test_file_path = src_dir.join("test_file.txt");
        println!("[Test] Creating test file: {:?}", test_file_path);

        let mut file = File::create(&test_file_path)?;
        writeln!(file, "Hello, Rust test!")?;
        file.sync_all()?; // Force flushing to disk so the operating system definitely emits an event

        // 6. Wait for a notification from the callback (with a timeout of 2 seconds,
        // in case the watcher fails for some reason and the test would otherwise hang indefinitely)
        let test_result =
            tokio::time::timeout(tokio::time::Duration::from_secs(2), notify.notified()).await;

        // 7. Check the test result
        assert!(
            test_result.is_ok(),
            "Test timed out! The watcher did not detect file creation."
        );

        println!("[Test] Notification received successfully. Cleaning up environment.");

        // Stop the watcher by aborting the thread (in real code, watcher_handle can be aborted)
        watcher_handle.abort();

        Ok(())
    }
}
