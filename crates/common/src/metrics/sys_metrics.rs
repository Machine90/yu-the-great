use std::{ffi::OsString, fmt::Display};

#[derive(Debug, Clone, Default)]
pub struct SysMetrics {
    pub pid: String,
    /// Start timestamp for process, in ms.
    pub start_ts: u64,
    /// Percentage of CPU usage.
    pub cpu_usage: f32,
    /// CPU load: (one, five, fifteen)
    pub cpu_load: (f64, f64, f64),
    pub cpu_logical_cores: u16,
    pub cpu_physical_cores: u16,
    /// Current process memory usage in KB
    pub memory_current: u64,
    pub memory_total: u64,
    pub memory_used: u64,
    pub memory_avail: u64,
    pub swap_total: u64,
    pub swap_used: u64,
    pub swap_avail: u64,
    pub disks: Vec<Disk>,
}

impl SysMetrics {

    #[inline]
    pub fn total_disks_space(&self) -> u64 {
        self.disks.iter().map(|d| d.total_bytes).sum()
    }

    #[inline]
    pub fn total_disks_used(&self) -> u64 {
        self.disks.iter().map(|d| d.total_bytes - d.avail_bytes).sum()
    }

    #[inline]
    pub fn total_disks_avail(&self) -> u64 {
        self.disks.iter().map(|d| d.avail_bytes).sum()
    }
}

impl Display for SysMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f, "System Info:\nPID: {}\nStart Timestamp: {}\nCPU Usage {}%, Logical Cores: {}, Physical Cores: {}\nCPU Load: (One: {}, Five: {}, Fifteen: {})\nMemory Usage: current({} KB), total({} KB), used({} KB), avail({} KB)", 
            self.pid,
            self.start_ts,
            self.cpu_usage, self.cpu_logical_cores, self.cpu_physical_cores,
            self.cpu_load.0, self.cpu_load.1, self.cpu_load.2,
            self.memory_current, self.memory_total, self.memory_used, self.memory_avail
        )?;
        let disks = self.disks.len();
        if disks > 0 {
            writeln!(f, "Mounted {} Disks:", disks)?;
        }
        for disk in &self.disks {
            writeln!(f, "{}", disk)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct Disk {
    pub mount: OsString,
    pub avail_bytes: u64,
    pub total_bytes: u64
}

impl Display for Disk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, 
            "[Mount: {:?}, total: {} bytes, avail: {} bytes]", 
            self.mount, self.total_bytes, self.avail_bytes
        )
    }
}