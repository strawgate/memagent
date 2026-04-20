//! InputSource backed by pre-loaded data for simulation testing.

use std::collections::VecDeque;
use std::io;

use logfwd_io::input::{InputEvent, InputSource};
use logfwd_io::tail::ByteOffset;
use logfwd_types::diagnostics::ComponentHealth;
use logfwd_types::pipeline::SourceId;

/// A mock InputSource that returns pre-loaded chunks one at a time.
pub struct ChannelInputSource {
    name: String,
    chunks: VecDeque<Vec<u8>>,
    source_id: SourceId,
    offset: u64,
}

impl ChannelInputSource {
    pub fn new(name: &str, source_id: SourceId, data: Vec<Vec<u8>>) -> Self {
        Self {
            name: name.to_string(),
            chunks: data.into(),
            source_id,
            offset: 0,
        }
    }
}

impl InputSource for ChannelInputSource {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        match self.chunks.pop_front() {
            Some(data) => {
                let accounted_bytes = data.len() as u64;
                self.offset += data.len() as u64;
                Ok(vec![InputEvent::Data {
                    bytes: data,
                    source_id: Some(self.source_id),
                    accounted_bytes,
                    cri_metadata: None,
                }])
            }
            None => Ok(vec![]),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn health(&self) -> ComponentHealth {
        // The turmoil simulation source is preloaded and has no independent
        // runtime lifecycle beyond the harness driving its queued chunks.
        ComponentHealth::Healthy
    }

    fn checkpoint_data(&self) -> Vec<(SourceId, ByteOffset)> {
        if self.offset > 0 {
            vec![(self.source_id, ByteOffset(self.offset))]
        } else {
            vec![]
        }
    }
}
