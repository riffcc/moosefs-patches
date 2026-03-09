use alloc::vec::Vec;

use crate::logic::{CompletedResponse, ControlQueue, CtrlQueueError};

#[derive(Debug, Default)]
pub struct CtrlDevice {
    queue: ControlQueue,
}

impl CtrlDevice {
    pub fn new() -> Self {
        Self {
            queue: ControlQueue::new(),
        }
    }

    pub fn helper_open(&mut self) {
        self.queue.helper_open();
    }

    pub fn helper_release(&mut self) {
        self.queue.helper_release();
    }

    pub fn helper_is_online(&self) -> bool {
        self.queue.helper_is_online()
    }

    pub fn submit_kernel_request(&mut self, op: u16, payload: &[u8]) -> Result<u32, CtrlQueueError> {
        self.queue.enqueue_call(op, payload)
    }

    pub fn helper_read_request(&mut self, max_count: usize) -> Result<Option<Vec<u8>>, CtrlQueueError> {
        self.queue.pop_next_request_frame(max_count)
    }

    pub fn helper_write_response(&mut self, frame: &[u8]) -> Result<(), CtrlQueueError> {
        self.queue.submit_response_frame(frame)
    }

    pub fn timeout_request(&mut self, req_id: u32) {
        self.queue.timeout_request(req_id);
    }

    pub fn take_completed(&mut self, req_id: u32) -> Option<CompletedResponse> {
        self.queue.take_response(req_id)
    }
}
