//! Persistent compiler future for one accepted connection.
use crate::config::READ_BYTES;
use crate::scheduler::{TaskContext, WaitReason};
use crate::{EndReason, Record, RecordKind, WireOffset};
use mio::net::TcpStream;
use phonowire_audiosocket::{
    DecodeOutcome, IncomingEvent, IncomingProfile, IncomingSession, RawDecoder, SessionEnd,
};
use std::future::poll_fn;
use std::io::{self, Read};
use std::task::Poll;
use std::time::Instant;

/// Runs one socket's persistent decoder and session state until its terminal outcome.
pub async fn receive(
    mut socket: TcpStream,
    payload_capacity: usize,
    profile: IncomingProfile,
    context: TaskContext,
) {
    receive_from(&mut socket, payload_capacity, profile, context).await;
}

async fn receive_from<R: Read>(
    mut socket: R,
    payload_capacity: usize,
    profile: IncomingProfile,
    context: TaskContext,
) {
    let mut scratch = vec![0; payload_capacity].into_boxed_slice();
    let mut decoder = RawDecoder::new(&mut scratch);
    let mut session = IncomingSession::with_profile(profile);
    let mut read = [0_u8; READ_BYTES];
    if !send(
        &context,
        0,
        RecordKind::Connected {
            peer: context.peer(),
        },
    )
    .await
    {
        return;
    }
    loop {
        let count = match read_once(&mut socket, &mut read, &context).await {
            Ok(count) => count,
            Err(error) => {
                let _ = end(
                    &context,
                    context.offset(),
                    session.identity(),
                    EndReason::Transport(error),
                )
                .await;
                return;
            }
        };
        if count == 0 {
            let uuid = session.identity();
            let reason = match decoder.finish() {
                Ok(()) => match session.end_of_input() {
                    Ok(IncomingEvent::Ended { .. }) => EndReason::CleanEof,
                    Ok(
                        IncomingEvent::Started(_)
                        | IncomingEvent::Audio { .. }
                        | IncomingEvent::Dtmf { .. },
                    ) => panic!("end_of_input must return a terminal event"),
                    Err(error) => EndReason::Policy(error),
                },
                Err(error) => EndReason::Truncated(error),
            };
            let _ = end(&context, context.offset(), uuid, reason).await;
            return;
        }
        let observed_at = Instant::now();
        let Ok(start) = context.observe_read(count) else {
            let _ = end(
                &context,
                context.offset(),
                session.identity(),
                EndReason::OffsetExhausted,
            )
            .await;
            return;
        };
        let bytes = match context.copy(&read[..count]).await {
            Ok(bytes) => bytes,
            Err(error) => {
                let _ = end(
                    &context,
                    start,
                    session.identity(),
                    EndReason::ResourceRefused(error),
                )
                .await;
                return;
            }
        };
        if !send_at(&context, start, observed_at, RecordKind::Wire { bytes }).await {
            return;
        }
        if !decode_chunk(&mut decoder, &mut session, &read[..count], &context).await {
            return;
        }
    }
}

async fn decode_chunk(
    decoder: &mut RawDecoder<'_>,
    session: &mut IncomingSession,
    bytes: &[u8],
    context: &TaskContext,
) -> bool {
    let mut input = bytes;
    while !input.is_empty() {
        context.step().await;
        let before = input.len();
        let frame = decoder.feed(&mut input);
        let consumed = before - input.len();
        let Ok(offset) = context.consume(consumed) else {
            let _ = end(
                context,
                context.offset(),
                session.identity(),
                EndReason::OffsetExhausted,
            )
            .await;
            return false;
        };
        match frame {
            Ok(DecodeOutcome::NeedInput) => return true,
            Err(error) => {
                let _ = end(
                    context,
                    offset,
                    session.identity(),
                    EndReason::Decode(error),
                )
                .await;
                return false;
            }
            Ok(DecodeOutcome::Frame(raw)) => {
                context.step().await;
                let typed = match raw.typed() {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = end(
                            context,
                            offset,
                            session.identity(),
                            EndReason::InvalidMessage(error),
                        )
                        .await;
                        return false;
                    }
                };
                let uuid = session.identity();
                let event = match session.receive(typed) {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = end(context, offset, uuid, EndReason::Policy(error)).await;
                        return false;
                    }
                };
                if !observe(context, offset, event).await || session.identity().is_none() {
                    return false;
                }
            }
        }
    }
    true
}

async fn read_once<R: Read>(
    socket: &mut R,
    read: &mut [u8; READ_BYTES],
    context: &TaskContext,
) -> io::Result<usize> {
    poll_fn(|cx| {
        if !context.permit(cx) {
            return Poll::Pending;
        }
        match socket.read(read) {
            Ok(count) => Poll::Ready(Ok(count)),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                context.park(WaitReason::Kernel);
                Poll::Pending
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(error) => Poll::Ready(Err(error)),
        }
    })
    .await
}

async fn observe(context: &TaskContext, offset: u64, event: IncomingEvent<'_>) -> bool {
    let observed_at = Instant::now();
    match event {
        IncomingEvent::Started(uuid) => {
            send_at(context, offset, observed_at, RecordKind::Started { uuid }).await
        }
        IncomingEvent::Dtmf { uuid, digit } => {
            send_at(
                context,
                offset,
                observed_at,
                RecordKind::Dtmf { uuid, digit },
            )
            .await
        }
        IncomingEvent::Audio {
            uuid,
            rate,
            payload,
        } => match context.copy(payload.bytes()).await {
            Ok(bytes) => {
                send_at(
                    context,
                    offset,
                    observed_at,
                    RecordKind::Audio { uuid, rate, bytes },
                )
                .await
            }
            Err(error) => {
                end_at(
                    context,
                    offset,
                    observed_at,
                    Some(uuid),
                    EndReason::ResourceRefused(error),
                )
                .await
            }
        },
        IncomingEvent::Ended {
            uuid,
            reason: SessionEnd::Terminate,
        } => end_at(context, offset, observed_at, uuid, EndReason::Terminate).await,
        IncomingEvent::Ended {
            uuid,
            reason: SessionEnd::PeerError(payload),
        } => match context.copy(payload.bytes()).await {
            Ok(bytes) => {
                end_at(
                    context,
                    offset,
                    observed_at,
                    uuid,
                    EndReason::PeerError(bytes),
                )
                .await
            }
            Err(error) => {
                end_at(
                    context,
                    offset,
                    observed_at,
                    uuid,
                    EndReason::ResourceRefused(error),
                )
                .await
            }
        },
        IncomingEvent::Ended {
            uuid,
            reason: SessionEnd::EndOfInput,
        } => end_at(context, offset, observed_at, uuid, EndReason::CleanEof).await,
    }
}

async fn end(
    context: &TaskContext,
    offset: u64,
    uuid: Option<phonowire_audiosocket::Uuid>,
    reason: EndReason,
) -> bool {
    send(context, offset, RecordKind::Ended { uuid, reason }).await
}
async fn end_at(
    context: &TaskContext,
    offset: u64,
    observed_at: Instant,
    uuid: Option<phonowire_audiosocket::Uuid>,
    reason: EndReason,
) -> bool {
    send_at(
        context,
        offset,
        observed_at,
        RecordKind::Ended { uuid, reason },
    )
    .await
}
async fn send(context: &TaskContext, offset: u64, kind: RecordKind) -> bool {
    send_at(context, offset, Instant::now(), kind).await
}
async fn send_at(
    context: &TaskContext,
    offset: u64,
    observed_at: Instant,
    kind: RecordKind,
) -> bool {
    context
        .send(Record {
            connection: context.id(),
            offset: WireOffset::new(offset),
            observed_at,
            kind,
        })
        .await
        .is_ok()
}

#[cfg(test)]
#[path = "connection_tests.rs"]
mod tests;
