use crate::connector::HookArgs;
use crate::connector::features::composite::stream::event::{
    StreamEvent, StreamEventContext, StreamEventParsed, StreamEventRoute, StreamEventUnion,
};
use crate::connector::features::grpc::stream::{GrpcDescriptor, GrpcEvent};
use crate::connector::features::http::stream::descriptor::HttpDescriptor;
use crate::connector::features::http::stream::event::HttpEvent;
use crate::connector::features::shared::events::{StreamEventInner, StreamEventRaw};
use crate::connector::features::websocket::stream::{WebSocketEvent, WebSocketStreamDescriptor};
use crate::io::mpmc::MpmcSender;
use crate::prelude::BaseTx;
use tracing::warn;

#[inline]
fn send_event<E: StreamEventParsed>(
    event_tx: &mut MpmcSender<StreamEvent<E>>,
    event: StreamEvent<E>,
    stream_type: &str,
) where
    E: StreamEventParsed,
{
    if let Err(err) = event_tx.try_send(event) {
        warn!(
            "{} hook failed to forward event from stream {:?}: {}",
            stream_type,
            err.value.as_ref().map(|v| v.stream.as_str()),
            err
        );
    }
}

#[inline]
fn process_event<E, RawEvent, Desc, R, State, F>(
    ctx: &StreamEventContext<E>,
    raw: StreamEventRaw<RawEvent>,
    desc: &Desc,
    event_tx: &mut MpmcSender<StreamEvent<E>>,
    reducer: &mut R,
    state: &crate::utils::StateCell<State>,
    stream_type: &str,
    use_parser: F,
) -> Option<StreamEventRaw<RawEvent>>
where
    RawEvent: StreamEventInner,
    E: StreamEventParsed,
    R: crate::utils::Reducer,
    State: crate::utils::StateMarker,
    F: FnOnce(
        StreamEventRaw<RawEvent>,
        &StreamEventRoute<E>,
        &Desc,
        &mut R,
        &crate::utils::StateCell<State>,
    ) -> Option<E>,
{
    if let Some(route) = ctx.select_route(raw.label(), raw.payload()) {
        if let Some(event) = use_parser(raw, &route, desc, reducer, state) {
            send_event(
                event_tx,
                StreamEvent {
                    stream: ctx.stream_name().clone(),
                    union: StreamEventUnion::Parsed(event),
                },
                stream_type,
            );
        }
        return None;
    }

    return Some(raw);
}

pub fn http_hook<E>(
    args: HookArgs<
        '_,
        StreamEventRaw<HttpEvent>,
        MpmcSender<StreamEvent<E>>,
        E::HttpReducer,
        E::HttpState,
        HttpDescriptor<StreamEventContext<E>>,
        StreamEventContext<E>,
    >,
) where
    E: StreamEventParsed,
{
    let ctx = args
        .desc
        .ctx
        .as_ref()
        .expect("http hook ctx is required for http hook");

    let maybe_raw = process_event(
        ctx,
        args.raw,
        args.desc,
        args.event_tx,
        args.reducer,
        args.state,
        "http",
        |raw, route, desc, reducer, state| {
            route.parser().from_http(raw, route, desc, reducer, state)
        },
    );

    if let Some(raw) = maybe_raw {
        send_event(
            args.event_tx,
            StreamEvent {
                stream: ctx.stream_name().clone(),
                union: StreamEventUnion::RawHttp(raw),
            },
            "http",
        );
    }
}

pub fn grpc_hook<E>(
    args: HookArgs<
        '_,
        StreamEventRaw<GrpcEvent>,
        MpmcSender<StreamEvent<E>>,
        E::GrpcReducer,
        E::GrpcState,
        GrpcDescriptor<StreamEventContext<E>>,
        StreamEventContext<E>,
    >,
) where
    E: StreamEventParsed,
{
    let ctx = args
        .desc
        .ctx
        .as_ref()
        .expect("grpc hook ctx is required for grpc hook");

    let maybe_raw = process_event(
        ctx,
        args.raw,
        args.desc,
        args.event_tx,
        args.reducer,
        args.state,
        "grpc",
        |raw, route, desc, reducer, state| {
            route.parser().from_grpc(raw, route, desc, reducer, state)
        },
    );

    if let Some(raw) = maybe_raw {
        send_event(
            args.event_tx,
            StreamEvent {
                stream: ctx.stream_name().clone(),
                union: StreamEventUnion::RawGrpc(raw),
            },
            "grpc",
        );
    }
}

pub fn ws_hook<E>(
    args: HookArgs<
        '_,
        StreamEventRaw<WebSocketEvent>,
        MpmcSender<StreamEvent<E>>,
        E::WsReducer,
        E::WsState,
        WebSocketStreamDescriptor<StreamEventContext<E>>,
        StreamEventContext<E>,
    >,
) where
    E: StreamEventParsed,
{
    let ctx = args
        .desc
        .ctx
        .as_ref()
        .expect("grpc hook ctx is required for grpc hook");

    let maybe_raw = process_event(
        ctx,
        args.raw,
        args.desc,
        args.event_tx,
        args.reducer,
        args.state,
        "ws",
        |raw, route, desc, reducer, state| route.parser().from_ws(raw, route, desc, reducer, state),
    );

    if let Some(raw) = maybe_raw {
        send_event(
            args.event_tx,
            StreamEvent {
                stream: ctx.stream_name().clone(),
                union: StreamEventUnion::RawWs(raw),
            },
            "ws",
        );
    }
}
