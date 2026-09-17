use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::usize;

use parser::ParsedCommand;

/// Fsync policy for AOF writes.
#[derive(Debug, Clone, PartialEq)]
pub enum AofFsyncPolicy {
    /// Fsync after every write (safest, slowest).
    Always,
    /// Fsync once per second (good balance of safety and performance).
    Everysec,
    /// Let the OS decide when to flush (fastest, least safe).
    No,
}

impl AofFsyncPolicy {
    /// Parse from a config string (e.g. "always", "everysec", "no").
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "always" => AofFsyncPolicy::Always,
            "no" => AofFsyncPolicy::No,
            _ => AofFsyncPolicy::Everysec, // default
        }
    }
}

pub struct Aof {
    fp: File,
    dbindex: usize,
    fsync_policy: AofFsyncPolicy,
}

impl Aof {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Aof> {
        Ok(Aof {
            fp: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?,
            dbindex: usize::MAX,
            fsync_policy: AofFsyncPolicy::Everysec, // default
        })
    }

    /// Create an AOF with a specific fsync policy.
    pub fn with_fsync_policy<P: AsRef<Path>>(path: P, policy: AofFsyncPolicy) -> io::Result<Aof> {
        Ok(Aof {
            fp: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?,
            dbindex: usize::MAX,
            fsync_policy: policy,
        })
    }

    /// Set the fsync policy.
    pub fn set_fsync_policy(&mut self, policy: AofFsyncPolicy) {
        self.fsync_policy = policy;
    }

    pub fn select(&mut self, dbindex: usize) -> io::Result<()> {
        if self.dbindex != dbindex {
            // TODO: use logarithms to know the length?
            let n = format!("{}", dbindex);
            write!(self.fp, "*2\r\n$6\r\nSELECT\r\n${}\r\n{}\r\n", n.len(), n)?;
            self.dbindex = dbindex;
        }
        Ok(())
    }

    pub fn truncate(&mut self, pos: usize) -> bool {
        if self.fp.set_len(pos as u64).is_err() {
            return false;
        }
        self.fp.seek(SeekFrom::Start(pos as u64)).is_ok()
    }

    pub fn write(&mut self, dbindex: usize, command: &ParsedCommand) -> io::Result<()> {
        self.select(dbindex)?;
        self.fp.write_all(command.get_data())?;
        match self.fsync_policy {
            AofFsyncPolicy::Always => {
                self.fp.flush()?;
                let _ = self.fp.sync_all();
            }
            AofFsyncPolicy::Everysec | AofFsyncPolicy::No => {
                // For everysec, we rely on the OS buffer and periodic flush.
                // For no, we just write and let the OS decide.
                self.fp.flush()?;
            }
        }
        Ok(())
    }

    /// Perform an explicit fsync (used by background fsync thread for everysec).
    pub fn do_fsync(&mut self) -> io::Result<()> {
        self.fp.sync_all()
    }
}

impl io::Read for Aof {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.fp.read(buf)
    }
}

#[cfg(test)]
mod test_aof {
    use std::env::temp_dir;
    use std::fs::File;
    use std::io::Read;
    use std::io::Write;

    use super::Aof;
    use parser::parse;

    #[test]
    fn test_write() {
        let mut path = temp_dir();
        path.push("aoftest");

        {
            let command = parse(b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n").unwrap().0;

            let mut w = Aof::new(path.as_path()).unwrap();
            w.write(10, &command).unwrap()
        }
        {
            let mut data = String::with_capacity(100);
            File::open(path.as_path())
                .unwrap()
                .read_to_string(&mut data)
                .unwrap();
            assert_eq!(
                data,
                "*2\r\n$6\r\nSELECT\r\n$2\r\n10\r\n*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
            );
        }
    }

    #[test]
    fn test_read() {
        let mut path = temp_dir();
        path.push("aoftest2");
        File::create(path.as_path())
            .unwrap()
            .write(b"hello world")
            .unwrap();

        let mut r = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '!' as u8];
        let mut aof = Aof::new(path.as_path()).unwrap();
        assert_eq!(11, aof.read(&mut r).unwrap());
        assert_eq!(&r, b"hello world!");
    }
}
