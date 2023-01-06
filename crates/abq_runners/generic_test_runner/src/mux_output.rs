//! Utilities for multiplexing child output to parent stdout/stderr, and into managed buffers.
#![allow(unused)]

use std::marker::PhantomData;
use std::sync::Arc;
use std::{pin::Pin, task};

use parking_lot::Mutex;
use tokio::io::{self, AsyncRead, AsyncWrite};
use tokio::task::JoinHandle;

struct Muxer<'a, P>
where
    P: AsyncWrite + Unpin + ?Sized,
{
    pipe: &'a mut P,
    side_channel: SideChannel,
}

impl<'a, P> Muxer<'a, P>
where
    P: AsyncWrite + Unpin + ?Sized,
{
    fn new(pipe: &'a mut P, side_channel: SideChannel) -> Self {
        Self { pipe, side_channel }
    }
}

#[derive(Default, Debug)]
struct SideChannelInner {
    capturing: bool,
    capture_buf: Vec<u8>,
    unassociated_output: Vec<u8>,
}

#[derive(Default, Clone, Debug)]
pub struct SideChannel(Arc<Mutex<SideChannelInner>>);

impl SideChannel {
    fn write(&self, buf: &[u8]) {
        let mut channel = self.0.lock();
        if channel.capturing {
            channel.capture_buf.extend(buf);
        } else {
            channel.unassociated_output.extend(buf);
        }
    }

    pub fn begin_capture(&self) {
        let mut channel = self.0.lock();
        assert!(
            !channel.capturing,
            "cannot call begin_capture while already capturing"
        );
        channel.capturing = true;
    }

    pub fn end_capture(&self) -> Vec<u8> {
        let mut channel = self.0.lock();
        assert!(
            channel.capturing,
            "cannot call end_capture while not capturing"
        );
        channel.capturing = false;
        std::mem::take(&mut channel.capture_buf)
    }

    /// Consumes the side channel and returns all output that was not associated with a capturing
    /// region.
    /// Returns an error if the side channel is not exclusively referenced.
    pub fn finish(self) -> Result<Vec<u8>, Self> {
        let channel = Arc::try_unwrap(self.0).map_err(Self)?;
        let channel = Mutex::into_inner(channel);
        assert!(
            !channel.capturing,
            "finish called on channel while still capturing"
        );
        Ok(channel.unassociated_output)
    }
}

pub struct MuxOutput<C, P> {
    pub copied_all_output: JoinHandle<io::Result<()>>,
    pub side_channel: SideChannel,
    _witness: PhantomData<(C, P)>,
}

/// Multiplexes outut from `child` to `parent` and into a [SideChannel].
/// Returns the [SideChannel] being written into, and spawns a tokio task that completes when all
/// output has been copied from the child to the parent.
pub fn mux_output<Child, Parent>(mut child: Child, mut parent: Parent) -> MuxOutput<Child, Parent>
where
    Child: AsyncRead + Unpin + Send + Sized + 'static,
    Parent: AsyncWrite + Unpin + Send + Sized + 'static,
{
    let side_channel = SideChannel::default();

    let copy_all_output_task = {
        let side_channel = side_channel.clone();
        async move {
            let mut mux_out = Muxer::new(&mut parent, side_channel);
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

impl<'a, P> AsyncWrite for Muxer<'a, P>
where
    P: AsyncWrite + Unpin + ?Sized,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &[u8],
    ) -> task::Poll<Result<usize, std::io::Error>> {
        self.side_channel.write(buf);
        Pin::new(&mut self.pipe).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.pipe).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.pipe).poll_shutdown(cx)
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

    use super::{mux_output, MuxOutput};

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
    async fn writes_to_parent() {
        let parent = SharedBuf::default();
        let inc = IncrementalReader::default();
        let MuxOutput {
            copied_all_output,
            side_channel: _,
            _witness,
        } = mux_output(inc.clone(), parent.clone());

        inc.push_buf(&[1, 2, 3]);
        inc.push_buf(&[4, 5, 6]);
        inc.finish();

        copied_all_output.await.unwrap().unwrap();

        assert_eq!(&*parent.read(), &[1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn writes_to_capture_buffer() {
        let parent = SharedBuf::default();
        let inc = IncrementalReader::default();
        let MuxOutput {
            copied_all_output,
            side_channel,
            _witness,
        } = mux_output(inc.clone(), parent.clone());

        {
            side_channel.begin_capture();

            inc.push_buf(&[1, 2, 3]);
            inc.push_buf(&[4, 5, 6]);
            wait_until(|| parent.read().ends_with(&[6])).await;

            assert_eq!(side_channel.end_capture(), &[1, 2, 3, 4, 5, 6]);
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
            side_channel,
            _witness,
        } = mux_output(inc.clone(), parent.clone());

        // Before any capture - should write to unassociated buffer
        {
            inc.push_buf(&[1, 2, 3]);
            wait_until(|| parent.read().ends_with(&[3])).await;
        }

        // Capture 1
        {
            side_channel.begin_capture();

            inc.push_buf(&[4, 5, 6]);
            wait_until(|| parent.read().ends_with(&[6])).await;

            assert_eq!(side_channel.end_capture(), &[4, 5, 6]);
        }

        // Between captures - should write to unassociated buffer
        {
            inc.push_buf(&[7, 8, 9]);
            wait_until(|| parent.read().ends_with(&[9])).await;
        }

        // Capture 2
        {
            side_channel.begin_capture();

            inc.push_buf(&[10, 11, 12]);
            wait_until(|| parent.read().ends_with(&[12])).await;

            assert_eq!(side_channel.end_capture(), &[10, 11, 12]);
        }

        // After all captures - should write to unassociated buffer
        {
            inc.push_buf(&[13, 14, 15]);
            wait_until(|| parent.read().ends_with(&[15])).await;
        }

        inc.finish();
        copied_all_output.await.unwrap().unwrap();

        let unassociated = side_channel.finish().unwrap();
        assert_eq!(unassociated, &[1, 2, 3, 7, 8, 9, 13, 14, 15]);
        assert_eq!(&*parent.read(), &(1..=15).collect::<Vec<_>>());
    }
}
