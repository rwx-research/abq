//! Utilities for multiplexing child output to parent stdout/stderr, and into managed buffers.
#![allow(unused)]

use std::borrow::BorrowMut;
use std::marker::PhantomData;
use std::sync::Arc;
use std::{pin::Pin, task};

use parking_lot::Mutex;
use tokio::io::{self, AsyncRead, AsyncWrite};
use tokio::task::JoinHandle;

struct Muxer<P>
where
    P: AsyncWrite + Unpin,
{
    pipe: Option<P>,
    side_channels: Vec<SideChannel>,
}

impl<P> Muxer<P>
where
    P: AsyncWrite + Unpin,
{
    fn new(pipe: Option<P>, side_channels: Vec<SideChannel>) -> Self {
        Self {
            pipe,
            side_channels,
        }
    }
}

impl<P> AsyncWrite for Muxer<P>
where
    P: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &[u8],
    ) -> task::Poll<Result<usize, std::io::Error>> {
        let poll = match self.pipe.borrow_mut() {
            Some(pipe) => Pin::new(pipe).poll_write(cx, buf),
            None => task::Poll::Ready(Ok(buf.len())),
        };

        if let task::Poll::Ready(Ok(num_bytes_written)) = poll {
            // Only read as many bytes as the underlying stream write, since we'll get polled
            // again for anything missed.
            for side_channel in self.side_channels.iter() {
                side_channel.write(&buf[..num_bytes_written]);
            }
        }
        poll
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), std::io::Error>> {
        match self.pipe.borrow_mut() {
            Some(pipe) => Pin::new(pipe).poll_flush(cx),
            None => task::Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), std::io::Error>> {
        match self.pipe.borrow_mut() {
            Some(pipe) => Pin::new(pipe).poll_shutdown(cx),
            None => task::Poll::Ready(Ok(())),
        }
    }
}

pub struct MuxOutput<C, P> {
    pub copied_all_output: JoinHandle<io::Result<()>>,
    pub side_channel: SideChannel,
    _witness: PhantomData<(C, P)>,
}

/// Multiplexes output from `child` to `parent` and into [SideChannel]s.
/// Spawns a tokio task that completes when all output has been copied
/// from the child to the parent.
pub fn mux_output<Child, Parent>(
    mut child: Child,
    parent: Option<Parent>,
    side_channel: SideChannel,
    combined_channel: SideChannel,
) -> MuxOutput<Child, Parent>
where
    Child: AsyncRead + Unpin + Send + Sized + 'static,
    Parent: AsyncWrite + Unpin + Send + Sized + 'static,
{
    let copy_all_output_task = {
        let side_channels = vec![side_channel.clone(), combined_channel];
        async move {
            let mut mux_out = Muxer::new(parent, side_channels);
            let _written_out = io::copy(&mut child, &mut mux_out).await?;
            io::Result::<()>::Ok(())
        }
    };
    let copy_all_output_handle = tokio::spawn(copy_all_output_task);

    MuxOutput {
        copied_all_output: copy_all_output_handle,
        side_channel,
        _witness: Default::default(),
    }
}

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
        let channel = self.0.lock();
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

    use super::{mux_output, MuxOutput, SideChannel};

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
        let parent = SharedBuf::default();
        let inc = IncrementalReader::default();
        let MuxOutput {
            copied_all_output,
            side_channel,
            _witness,
        } = mux_output(
            inc.clone(),
            Some(parent.clone()),
            SideChannel::default(),
            SideChannel::default(),
        );

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
        let parent = SharedBuf::default();
        let inc = IncrementalReader::default();
        let MuxOutput {
            copied_all_output,
            mut side_channel,
            _witness,
        } = mux_output(
            inc.clone(),
            Some(parent.clone()),
            SideChannel::default(),
            SideChannel::default(),
        );

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

    #[tokio::test]
    async fn writes_to_parent_and_side_channels() {
        let parent = SharedBuf::default();
        let inc = IncrementalReader::default();
        let side1 = SideChannel::default();
        let side2 = SideChannel::default();
        let MuxOutput {
            copied_all_output,
            side_channel: _,
            _witness,
        } = mux_output(
            inc.clone(),
            Some(parent.clone()),
            side1.clone(),
            side2.clone(),
        );

        inc.push_buf(&[1, 2, 3]);
        inc.push_buf(&[4, 5, 6]);
        inc.finish();

        copied_all_output.await.unwrap().unwrap();

        assert_eq!(&*parent.read(), &[1, 2, 3, 4, 5, 6]);
        assert_eq!(side1.get_captured(), &[1, 2, 3, 4, 5, 6]);
        assert_eq!(side2.get_captured(), &[1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn writes_to_side_channels_without_parent() {
        let inc = IncrementalReader::default();
        let side1 = SideChannel::default();
        let side2 = SideChannel::default();
        let MuxOutput {
            copied_all_output,
            side_channel: _,
            _witness,
        } = mux_output(inc.clone(), None::<SharedBuf>, side1.clone(), side2.clone());

        inc.push_buf(&[1, 2, 3]);
        inc.push_buf(&[4, 5, 6]);
        inc.finish();

        copied_all_output.await.unwrap().unwrap();

        assert_eq!(side1.get_captured(), &[1, 2, 3, 4, 5, 6]);
        assert_eq!(side2.get_captured(), &[1, 2, 3, 4, 5, 6]);
    }
}
