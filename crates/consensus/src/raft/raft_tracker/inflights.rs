use std::fmt::Display;

/// The inflights keep all messages that broadcast append by leader raft 
/// to all followers but not received append response yet. the buffer in 
/// the "Inflights" is a ringbuffer
#[derive(Debug, PartialEq)]
pub struct RingBuffer {
    start: usize,
    count: usize,
    buffer: Vec<u64>
}

impl RingBuffer {

    pub fn new(cap: usize) -> Self {
        RingBuffer {
            buffer: Vec::with_capacity(cap),
            count: 0,
            start: 0
        }
    }

    fn next_pos(&self) -> usize {
        let mut next_pos = self.start + self.count;
        if next_pos >= self.capacity() {
            next_pos -= self.capacity();
        }
        next_pos
    }
}

pub trait Inflights {

    fn capacity(&self) -> usize;

    fn is_full(&self) -> bool;

    /// Insert inflight to inflights ring buffer in order
    fn push_back(&mut self, inflight: u64);

    /// Release buffer to given watermark "to_inflight"
    /// # Example
    /// \[8,9,0,1,3,5\] start: 2, count: 4, to_inflight: 3
    /// then after release... start: 5, count: 1 <br/>
    /// It's equal release \[1,3,5\] to \[5\]
    /// ```
    /// let mut ring_buffer = RingBuffer {start: 2, count: 4, buffer: vec![8,9,0,1,3,5]};
    /// ring_buffer.release_to(3);
    /// assert!(ring_buffer.start == 5 && ring_buffer.count = 1);
    ///```
    fn release_to(&mut self, to_inflight: u64);

    fn pop_front(&mut self) -> Option<u64>;

    fn clear(&mut self);
}

/// We should keep the actual capacity of buffer after clone.
/// Defatult Clone will not keep the capacity of the buffer
impl Clone for RingBuffer {
    fn clone(&self) -> Self {
        let mut buffer = self.buffer.clone();
        buffer.reserve(self.capacity() - self.buffer.len());
        RingBuffer { start: self.start, count: self.count, buffer }
    }
}

impl Display for RingBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let start = self.start;
        let count = self.count;
        let cap = self.capacity();
        let next = self.next_pos();

        if start + count > 20 {
            return write!(f, "start: {}, count: {}, buffer: [{} .. {}]", start, count, self.buffer[start], self.buffer[next]);
        }

        let mut buffer = Vec::new();
        if start + count > cap {
            let f = self.buffer.get(start .. cap).unwrap().clone(); 
            let s = self.buffer.get(0 .. next).unwrap();
            buffer.extend_from_slice(f);
            buffer.extend_from_slice(s);
        } else {
            buffer.extend_from_slice(self.buffer.get(start .. (start + count)).unwrap());
        };
        write!(f, "{:?}", buffer)
    }
}

/// This is the inflight impl for bing buffer which hold all
/// inflights messages in a ring buffer in order.
impl Inflights for RingBuffer {

    #[inline] fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    #[inline] fn is_full(&self) -> bool {
        self.capacity() == self.count
    }

    #[inline] fn push_back(&mut self, inflight: u64) {
        if self.is_full() {
            panic!("Inflight buffer is full, please release some buffer before use")
        }

        let next_pos = self.next_pos();
        assert!(next_pos <= self.buffer.len());

        let in_first_lap: bool = next_pos == self.buffer.len();
        if in_first_lap {
            self.buffer.push(inflight);
        } else {
            self.buffer[next_pos] = inflight;
        }
        self.count += 1;
    }
    
    #[inline] fn release_to(&mut self, to_inflight: u64) {
        let mut cursor = self.start;
        let mut step = 0usize;
        while step < self.count {
            // break out when reach watermark
            if to_inflight < self.buffer[cursor] { break; }
            cursor += 1;
            if cursor >= self.capacity() {
                cursor -= self.capacity();
            }
            step += 1;
        }
        self.start = cursor;
        self.count -= step;
    }

    #[inline] fn pop_front(&mut self) -> Option<u64> {
        if self.buffer.is_empty() {
            return None;
        }
        let pop = self.buffer[self.start];
        self.release_to(pop);
        Some(pop)
    }

    fn clear(&mut self) {
        self.start = 0;
        self.count = 0;
    }
}

#[test]
pub fn test_inflight() {
    let mut ring_buffer = RingBuffer {start: 2, count: 6, buffer: vec![8,9,0,1,3,5]};
    ring_buffer.release_to(3);
    ring_buffer.push_back(10);
    let front = ring_buffer.pop_front();
    println!("{} {:?}", ring_buffer, front.unwrap());
}
