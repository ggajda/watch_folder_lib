use anyhow::Result;
use notify::Event;
use std::fs::File;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

use watch_folder_lib::run_watch;

#[tokio::test]
async fn test_run_watch_detects_file_creation() -> Result<()> {
    let tmp_src = tempdir()?;
    let tmp_dst = tempdir()?;

    let src_path = tmp_src.path().to_path_buf();
    let dst_path = tmp_dst.path().to_path_buf();

    let received_event_paths = Arc::new(Mutex::new(Vec::new()));

    let cloned_paths = received_event_paths.clone();

    let src_path_thread = src_path.clone();
    let dst_path_thread = dst_path.clone();

    thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async move {
            let callback = move |_dst, event: Event| {
                let cloned_paths = cloned_paths.clone();

                async move {
                    let mut paths = cloned_paths.lock().unwrap();

                    for p in event.paths {
                        paths.push(p);
                    }

                    Ok(())
                }
            };

            let _ = run_watch(&src_path_thread, &dst_path_thread, callback).await;
        });
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    let test_file_path = src_path.join("test_file.txt");
    File::create(&test_file_path)?;

    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(100)).await;

        if received_event_paths
            .lock()
            .unwrap()
            .contains(&test_file_path)
        {
            break;
        }
    }

    let paths = received_event_paths.lock().unwrap();

    assert!(
        paths.contains(&test_file_path),
        "Detected paths: {:?}",
        *paths
    );

    Ok(())
}
