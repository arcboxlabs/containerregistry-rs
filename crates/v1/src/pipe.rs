//! Bridges synchronous producers (the `tar` crate) into async streams.

use std::io::{self, Write};

use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;
use tokio_util::io::StreamReader;

use crate::layer::BoxReader;

const CHUNK: usize = 256 * 1024;

/// A `Write` that forwards buffered chunks to an async reader.
pub(crate) struct PipeWriter {
    tx: mpsc::Sender<io::Result<Bytes>>,
    buf: BytesMut,
}

impl PipeWriter {
    fn send(&mut self, chunk: io::Result<Bytes>) -> io::Result<()> {
        self.tx
            .blocking_send(chunk)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "reader dropped"))
    }
}

impl Write for PipeWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(data);
        if self.buf.len() >= CHUNK {
            self.flush()?;
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            let chunk = self.buf.split().freeze();
            self.send(Ok(chunk))?;
        }
        Ok(())
    }
}

/// Runs `produce` on a blocking thread and returns a reader over what it
/// writes. An error returned by `produce` surfaces as a read error, so a
/// failed producer never looks like a short, successful stream.
pub(crate) fn blocking_pipe<F>(produce: F) -> BoxReader
where
    F: FnOnce(&mut PipeWriter) -> io::Result<()> + Send + 'static,
{
    let (tx, rx) = mpsc::channel::<io::Result<Bytes>>(4);
    tokio::task::spawn_blocking(move || {
        let mut w = PipeWriter {
            tx,
            buf: BytesMut::with_capacity(CHUNK),
        };
        let result = produce(&mut w).and_then(|()| w.flush());
        if let Err(e) = result {
            let _ = w.tx.blocking_send(Err(e));
        }
    });
    let stream = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });
    Box::pin(StreamReader::new(stream))
}
