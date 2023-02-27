//! Utilities for multiplexing child output to parent stdout/stderr, and into managed buffers.
#![allow(unused)]

use std::marker::PhantomData;
use std::sync::Arc;
use std::{pin::Pin, task};

use parking_lot::Mutex;
use tokio::io::{self, AsyncRead, AsyncWrite};
use tokio::task::JoinHandle;

#[derive(Default, Debug)]
struct SideChannelInner {
    buf: Vec<u8>,
}

#[derive(Default, Clone, Debug)]
pub struct SideChannel(Arc<Mutex<SideChannelInner>>);

impl SideChannel {
    fn write(&self, buf: &[u8]) {
        let mut channel = self.0.lock();
        channel.buf.extend(buf);
    }

    pub fn get_captured(&self) -> Vec<u8> {
        let mut channel = self.0.lock();
        let new_buf = Vec::with_capacity(channel.buf.len());
        std::mem::replace(&mut channel.buf, new_buf)
    }

    /// Like [Self::get_captured], but does not slice off the captured output from the internal
    /// buffer. Subsequent calls to `get_captured_ref`/`get_captured`/`finish` are supersets of the
    /// first call to `_ref`.
    pub fn get_captured_ref(&self) -> Vec<u8> {
        let mut channel = self.0.lock();
        channel.buf.clone()
    }

    /// Consumes the side channel and returns all remaining output.
    /// Returns an error if the side channel is not exclusively referenced.
    ///
    /// It is an error to call `finish` twice. However, `finish` does not take ownership because we
    /// often spawn multiple sidechannels in the same memory location.
    pub fn finish(&mut self) -> Result<Vec<u8>, Self> {
        let channel = std::mem::take(&mut self.0);
        let channel = Arc::try_unwrap(channel).map_err(Self)?;
        let channel = Mutex::into_inner(channel);
        Ok(channel.buf)
    }
}

impl AsyncWrite for SideChannel {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &[u8],
    ) -> task::Poll<Result<usize, std::io::Error>> {
        self.write(buf);
        task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), std::io::Error>> {
        task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), std::io::Error>> {
        task::Poll::Ready(Ok(()))
    }
}

pub struct OutputCapturer<C> {
    pub copied_all_output: JoinHandle<io::Result<()>>,
    pub side_channel: SideChannel,
    _witness: PhantomData<C>,
}

/// Captures from `child` into a [SideChannel].
/// Returns the [SideChannel] being written into, and spawns a tokio task that completes when all
/// output has been copied from the child to the parent.
pub fn capture_output<Child>(mut child: Child) -> OutputCapturer<Child>
where
    Child: AsyncRead + Unpin + Send + Sized + 'static,
{
    let side_channel = SideChannel::default();

    let copy_all_output_task = {
        let side_channel = side_channel.clone();
        async move {
            let mut side_channel = side_channel;
            let _written_out = io::copy(&mut child, &mut side_channel).await?;
            io::Result::<()>::Ok(())
        }
    };
    let copy_all_output_handle = tokio::spawn(copy_all_output_task);

    OutputCapturer {
        copied_all_output: copy_all_output_handle,
        side_channel,
        _witness: Default::default(),
    }
}

#[cfg(test)]
mod test {
    use std::{
        pin::Pin,
        sync::Arc,
        task::{self, Poll, Waker},
        time::Duration,
    };

    use parking_lot::{Mutex, MutexGuard};
    use tokio::io::{self, AsyncRead, AsyncWrite};

    use super::{capture_output, OutputCapturer};

    #[derive(Default, Clone)]
    struct SharedBuf {
        buf: Arc<Mutex<Vec<u8>>>,
    }

    impl SharedBuf {
        fn read(&self) -> MutexGuard<Vec<u8>> {
            self.buf.lock()
        }
    }

    impl AsyncWrite for SharedBuf {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut task::Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, std::io::Error>> {
            let mut me = self.buf.lock();
            Pin::new(&mut *me).poll_write(cx, buf)
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            cx: &mut task::Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            let mut me = self.buf.lock();
            Pin::new(&mut *me).poll_flush(cx)
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            cx: &mut task::Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            let mut me = self.buf.lock();
            Pin::new(&mut *me).poll_shutdown(cx)
        }
    }

    #[derive(Default)]
    struct IncrementalReaderInner {
        waker: Option<Waker>,
        buf: Vec<u8>,
        done: bool,
    }

    #[derive(Default, Clone)]
    struct IncrementalReader(Arc<Mutex<IncrementalReaderInner>>);

    impl IncrementalReader {
        fn push_buf(&self, buf: &[u8]) {
            let mut me = self.0.lock();
            me.buf.extend(buf);
            if let Some(waker) = std::mem::take(&mut me.waker) {
                waker.wake();
            }
        }

        fn finish(&self) {
            let mut me = self.0.lock();
            me.done = true;
            if let Some(waker) = std::mem::take(&mut me.waker) {
                waker.wake();
            }
        }
    }

    impl AsyncRead for IncrementalReader {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut task::Context<'_>,
            buf: &mut io::ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let mut me = self.0.lock();
            if me.buf.is_empty() && !me.done {
                me.waker = Some(cx.waker().clone());
                return Poll::Pending;
            }
            let read_amt = me.buf.len().min(buf.remaining());
            let read = {
                let mut rest = me.buf.split_off(read_amt);
                std::mem::swap(&mut rest, &mut me.buf);
                rest
            };
            buf.put_slice(&read);
            Poll::Ready(Ok(()))
        }
    }

    async fn wait_until(f: impl Fn() -> bool) {
        while !f() {
            tokio::time::sleep(Duration::from_micros(10)).await;
        }
    }

    #[tokio::test]
    async fn writes_to_capture_buffer() {
        let inc = IncrementalReader::default();
        let OutputCapturer {
            copied_all_output,
            side_channel,
            _witness,
        } = capture_output(inc.clone());

        {
            inc.push_buf(&[1, 2, 3]);
            inc.push_buf(&[4, 5, 6]);
            wait_until(|| side_channel.get_captured_ref().ends_with(&[6])).await;

            assert_eq!(side_channel.get_captured(), &[1, 2, 3, 4, 5, 6]);
        }

        inc.finish();
        copied_all_output.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn writes_to_unassociated_buffer() {
        let inc = IncrementalReader::default();
        let OutputCapturer {
            copied_all_output,
            mut side_channel,
            _witness,
        } = capture_output(inc.clone());

        // First write - should end up in Capture 1
        {
            inc.push_buf(&[1, 2, 3]);
            wait_until(|| side_channel.get_captured_ref().ends_with(&[3])).await;
        }

        // Capture 1
        {
            inc.push_buf(&[4, 5, 6]);
            wait_until(|| side_channel.get_captured_ref().ends_with(&[6])).await;

            assert_eq!(side_channel.get_captured(), &[1, 2, 3, 4, 5, 6]);
        }

        // write - should end up in Capture 2
        {
            inc.push_buf(&[7, 8, 9]);
            wait_until(|| side_channel.get_captured_ref().ends_with(&[9])).await;
        }

        // Capture 2
        {
            inc.push_buf(&[10, 11, 12]);
            wait_until(|| side_channel.get_captured_ref().ends_with(&[12])).await;

            assert_eq!(side_channel.get_captured(), &[7, 8, 9, 10, 11, 12]);
        }

        // After all above captures - should be resolved on finish
        {
            inc.push_buf(&[13, 14, 15]);
            wait_until(|| side_channel.get_captured_ref().ends_with(&[15])).await;
        }

        inc.finish();
        copied_all_output.await.unwrap().unwrap();

        let rest = side_channel.finish().unwrap();
        assert_eq!(rest, &[13, 14, 15]);
    }
}
