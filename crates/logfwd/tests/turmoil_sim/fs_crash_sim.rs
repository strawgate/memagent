//! Filesystem crash-consistency tests using Turmoil's unstable-fs shim.

use std::future::pending;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use turmoil::fs::shim::std::fs::{OpenOptions, sync_dir};

#[test]
fn synced_checkpoint_survives_crash_while_unsynced_append_is_lost() {
    let mut builder = super::sim_builder_with_profile(super::SimProfile::DEFAULT.with_duration(30));
    builder.fs().sync_probability(0.0);
    let mut sim = builder.build();

    let boot = Arc::new(AtomicUsize::new(0));
    let observations = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

    let boot_for_host = Arc::clone(&boot);
    let observations_for_host = Arc::clone(&observations);
    sim.host("storage", move || {
        let boot_for_host = Arc::clone(&boot_for_host);
        let observations_for_host = Arc::clone(&observations_for_host);
        async move {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open("/checkpoint.state")?;

            let mut current = String::new();
            file.seek(SeekFrom::Start(0))?;
            file.read_to_string(&mut current)?;
            observations_for_host
                .lock()
                .expect("mutex poisoned")
                .push(current);

            let boot_idx = boot_for_host.fetch_add(1, Ordering::SeqCst);
            match boot_idx {
                0 => {
                    // Durable base checkpoint.
                    file.set_len(0)?;
                    file.seek(SeekFrom::Start(0))?;
                    file.write_all(b"synced")?;
                    file.sync_all()?;
                    // Persist directory entry metadata as part of durability
                    // semantics for newly created files.
                    sync_dir("/")?;
                }
                1 => {
                    // Pending update without sync should be lost on crash.
                    file.seek(SeekFrom::End(0))?;
                    file.write_all(b"+unsynced")?;
                }
                _ => {}
            }

            pending::<()>().await;
            #[allow(unreachable_code)]
            Ok(())
        }
    });

    // Boot 0: write synced state.
    sim.step().expect("step after boot 0 should succeed");

    // Crash and restart.
    sim.crash("storage");
    sim.step().expect("step after crash 0 should succeed");
    sim.bounce("storage");
    sim.step().expect("step after bounce 0 should succeed");

    // Boot 1: append unsynced data, then crash and restart again.
    sim.crash("storage");
    sim.step().expect("step after crash 1 should succeed");
    sim.bounce("storage");
    sim.step().expect("step after bounce 1 should succeed");

    let obs = observations.lock().expect("mutex poisoned");
    assert!(
        obs.len() >= 3,
        "expected three boot observations, got {}",
        obs.len()
    );
    assert_eq!(obs[0], "", "fresh filesystem should start empty");
    assert_eq!(
        obs[1], "synced",
        "first restart should observe synced durable checkpoint"
    );
    assert_eq!(
        obs[2], "synced",
        "unsynced append must be discarded across crash+bounce"
    );
}

#[test]
fn synced_append_remains_durable_after_crash_bounce() {
    let mut builder = super::sim_builder_with_profile(super::SimProfile::DEFAULT.with_duration(30));
    builder.fs().sync_probability(0.0);
    let mut sim = builder.build();

    let boot = Arc::new(AtomicUsize::new(0));
    let observations = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

    let boot_for_host = Arc::clone(&boot);
    let observations_for_host = Arc::clone(&observations);
    sim.host("storage", move || {
        let boot_for_host = Arc::clone(&boot_for_host);
        let observations_for_host = Arc::clone(&observations_for_host);
        async move {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open("/checkpoint.state")?;

            let mut current = String::new();
            file.seek(SeekFrom::Start(0))?;
            file.read_to_string(&mut current)?;
            observations_for_host
                .lock()
                .expect("mutex poisoned")
                .push(current);

            match boot_for_host.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    file.set_len(0)?;
                    file.seek(SeekFrom::Start(0))?;
                    file.write_all(b"base")?;
                    file.sync_all()?;
                    sync_dir("/")?;
                }
                1 => {
                    file.seek(SeekFrom::End(0))?;
                    file.write_all(b"+synced")?;
                    file.sync_all()?;
                }
                _ => {}
            }

            pending::<()>().await;
            #[allow(unreachable_code)]
            Ok(())
        }
    });

    sim.step().expect("step after boot 0 should succeed");
    sim.crash("storage");
    sim.step().expect("step after crash 0 should succeed");
    sim.bounce("storage");
    sim.step().expect("step after bounce 0 should succeed");

    sim.crash("storage");
    sim.step().expect("step after crash 1 should succeed");
    sim.bounce("storage");
    sim.step().expect("step after bounce 1 should succeed");

    let obs = observations.lock().expect("mutex poisoned");
    assert!(
        obs.len() >= 3,
        "expected three boot observations, got {}",
        obs.len()
    );
    assert_eq!(obs[0], "", "fresh filesystem should start empty");
    assert_eq!(obs[1], "base", "first restart should observe durable base");
    assert_eq!(
        obs[2], "base+synced",
        "synced append should survive crash+bounce"
    );
}
