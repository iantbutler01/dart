macro_rules! in_temp_dir {
    ($block:expr) => {{
        let tmpdir = tempfile::tempdir().unwrap();
        let tmpdir_path = tmpdir.path();
        let wal_path = tmpdir_path.display().to_string() + "/wal";
        let disk_path = tmpdir_path.display().to_string() + "/disk";

        std::fs::create_dir(wal_path).expect("Expected WAL path to create in TempDir.");
        std::fs::create_dir(disk_path).expect("Expected Disk path to create in TempDir.");
        std::env::set_current_dir(&tmpdir).unwrap();

        $block(tmpdir.path().display().to_string());
    }};
}

pub(crate) use in_temp_dir;
