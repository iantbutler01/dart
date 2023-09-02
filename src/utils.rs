macro_rules! in_temp_dir {
    ($block:expr) => {{
        let tmpdir = tempfile::tempdir().unwrap();
        std::env::set_current_dir(&tmpdir).unwrap();

        $block(tmpdir.path().display().to_string());
    }};
}

pub(crate) use in_temp_dir;
