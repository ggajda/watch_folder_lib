// tests/test.rs

// Import public items from our library
use watch_folder_lib::run_watch;

// Import required external crates
use anyhow::Result;
//use notify::Event;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn test_run_watch_detects_file_creation() -> Result<()> {
    // Create temporary directories
    let tmp_src = tempdir()?;
    let tmp_dst = tempdir()?;

    let src_path = tmp_src.path().to_path_buf();
    let dst_path = tmp_dst.path().to_path_buf();

    let received_event_paths = Arc::new(Mutex::new(Vec::new()));
    //let cloned_paths = Arc::clone(&received_event_paths);

    let src_path_thread = src_path.clone();
    let dst_path_thread = dst_path.clone();

    // Run the watcher in a separate thread
    let _watcher_thread = thread::spawn(move || {
        let test_callback = move |_src: &Path, _dst: &Path| -> Result<()> {
            //let mut paths = cloned_paths.lock().unwrap();
            // for p in event.paths {
            //     paths.push(p);
            // }
            Ok(())
        };

        // Call the function from our library
        let _ = run_watch(&src_path_thread, &dst_path_thread, test_callback);
    });

    // Give the watcher a moment to start
    thread::sleep(Duration::from_millis(100));

    // Perform a file write action
    let test_file_path = src_path.join("test_file.txt");
    let _file = File::create(&test_file_path)?;

    // Wait for the event to be detected (up to 1 second)
    let mut attempts = 0;
    while attempts < 10 {
        thread::sleep(Duration::from_millis(100));
        let paths = received_event_paths.lock().unwrap();
        if !paths.is_empty() {
            break;
        }
        attempts += 1;
    }

    // Assertion
    let final_paths = received_event_paths.lock().unwrap();
    assert!(!final_paths.is_empty(), "No event was detected!");
    assert!(
        final_paths.contains(&test_file_path),
        "Detected paths {:?} do not contain the expected file {:?}",
        final_paths,
        test_file_path
    );

    Ok(())
}
