use crate::utils::RouteMatcher;
use crate::utils::StringTokens;
use crate::utils::{Reducer, SharedStr, StateMarker};
use crate::{
    connector::features::{
        grpc::stream::{GrpcDescriptor, GrpcEvent},
        http::stream::{descriptor::HttpDescriptor, event::HttpEvent},
        shared::events::StreamEventRaw,
        websocket::stream::{WebSocketEvent, WebSocketStreamDescriptor},
    },
    utils::{NullReducer, NullState},
};
use ahash::AHashMap;
use serde_json::Value;
use std::fmt::Debug;
use std::sync::Arc;

use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct EventRouteRegistry<E> {
    map: HashMap<SharedStr, Vec<StreamEventRoute<E>>>,
}

impl<E> EventRouteRegistry<E> {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn add_route(
        mut self,
        stream_name: impl Into<SharedStr>,
        route: StreamEventRoute<E>,
    ) -> Self {
        let stream_name = stream_name.into();
        self.map
            .entry(stream_name)
            .or_insert_with(Vec::new)
            .push(route);
        self
    }

    pub fn add_routes(
        mut self,
        stream_name: impl Into<SharedStr>,
        routes: Vec<StreamEventRoute<E>>,
    ) -> Self {
        let stream_name = stream_name.into();
        self.map
            .entry(stream_name)
            .or_insert_with(Vec::new)
            .extend(routes);
        self
    }

    pub fn get_routes(&self, stream_name: &str) -> Option<&Vec<StreamEventRoute<E>>> {
        self.map.get(stream_name)
    }

    pub fn get_routes_mut(&mut self, stream_name: &str) -> Option<&mut Vec<StreamEventRoute<E>>> {
        self.map.get_mut(stream_name)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&SharedStr, &Vec<StreamEventRoute<E>>)> {
        self.map.iter()
    }
}

/// Context shared between stream events from hooks and strategies.
#[derive(Debug, Clone)]
pub struct StreamEventContext<E> {
    stream: SharedStr,
    routes: Vec<StreamEventRoute<E>>,
    tokens: StringTokens,
    metadata: AHashMap<String, Value>,
}

impl<E> StreamEventContext<E> {
    pub fn new(stream: &str) -> anyhow::Result<Self> {
        let tokens = StringTokens::parse(stream);

        let metadata = AHashMap::new();

        let ctx = StreamEventContext {
            stream: stream.into(),
            routes: vec![],
            tokens,
            metadata,
        };

        Ok(ctx)
    }
    /// Stream identifier (stable within runtime).
    #[inline]
    pub fn stream_name(&self) -> &SharedStr {
        &self.stream
    }

    /// Tokens of the stream name
    #[inline]
    pub fn tokens(&self) -> &StringTokens {
        &self.tokens
    }
    /// Mut tokens of the stream name
    #[inline]
    pub fn tokens_mut(&mut self) -> &mut StringTokens {
        &mut self.tokens
    }

    /// Snapshot of global metadata.
    #[inline]
    pub fn metadata(&self) -> &AHashMap<String, Value> {
        &self.metadata
    }

    /// Mutate global metadata in-place.
    pub fn update_metadata<F>(&mut self, update: F)
    where
        F: FnOnce(&mut AHashMap<String, Value>),
    {
        update(&mut self.metadata);
    }

    /// Iterator over all configured routes.
    pub fn routes(&self) -> impl Iterator<Item = &StreamEventRoute<E>> + '_ {
        self.routes.iter()
    }

    /// Look up a route by identifier.
    pub fn route(&self, id: &str) -> Option<&StreamEventRoute<E>> {
        self.routes
            .iter()
            .find(|route| route.id().as_str().eq_ignore_ascii_case(id))
    }

    /// Add a new route.
    /// If a route with the same identifier already exists or matcher already exists, it will be replaced.
    pub fn add_route(&mut self, route: StreamEventRoute<E>) {
        self.routes.retain(|r| {
            !r.id().as_str().eq_ignore_ascii_case(route.id().as_str()) && r.matcher != route.matcher
        });

        self.routes.push(route);
    }

    /// Select a route based on label and payload.
    #[inline(always)]
    pub fn select_route(
        &self,
        label: Option<&str>,
        payload: Option<&Value>,
    ) -> Option<&StreamEventRoute<E>> {
        self.routes
            .iter()
            .find(|route| route.matches(label, payload))
    }
}

#[derive(Clone, Debug)]
pub struct StreamEventRoute<E> {
    id: SharedStr,
    matcher: RouteMatcher,
    parser: Arc<dyn StreamEventParser<E>>,
}

impl<E> StreamEventRoute<E> {
    pub fn new(
        id: impl Into<SharedStr>,
        matcher: RouteMatcher,
        parser: impl StreamEventParser<E>,
    ) -> Self {
        let route = Self {
            id: id.into(),
            matcher: matcher,
            parser: Arc::new(parser),
        };
        route
    }
    #[inline(always)]
    pub fn id(&self) -> &SharedStr {
        &self.id
    }
    #[inline(always)]
    pub fn matches(&self, label: Option<&str>, payload: Option<&Value>) -> bool {
        self.matcher.matches(label, payload)
    }
    #[inline(always)]
    pub fn parser(&self) -> &Arc<dyn StreamEventParser<E>> {
        &self.parser
    }
}

/// Adapters translate raw connector events into unified trading payloads.
pub trait StreamEventParser<E>: Debug + Send + Sync + 'static {
    /// Human readable identifier.
    fn name(&self) -> &str;

    /// Convert an HTTP stream event into a trading payload.
    fn from_http(
        &self,
        _event: StreamEventRaw<HttpEvent>,
        _route: &StreamEventRoute<E>,
        _desc: &HttpDescriptor<StreamEventContext<E>>,
    ) -> Option<E> {
        None
    }

    /// Convert a gRPC stream event into a trading payload.
    fn from_grpc(
        &self,
        _event: StreamEventRaw<GrpcEvent>,
        _route: &StreamEventRoute<E>,
        _desc: &GrpcDescriptor<StreamEventContext<E>>,
    ) -> Option<E> {
        None
    }

    /// Convert a WebSocket stream event into a trading payload.
    fn from_ws(
        &self,
        _event: StreamEventRaw<WebSocketEvent>,
        _route: &StreamEventRoute<E>,
        _desc: &WebSocketStreamDescriptor<StreamEventContext<E>>,
    ) -> Option<E> {
        None
    }
}

pub trait StreamEventParsed: Debug + Send + Clone + 'static {
    /// Associated type for the reducer used in stream hooks
    type HttpReducer: Reducer;
    type GrpcReducer: Reducer;
    type WsReducer: Reducer;

    /// Associated type for the state used in stream hooks
    type HttpState: StateMarker;
    type GrpcState: StateMarker;
    type WsState: StateMarker;
}

#[derive(Clone, Debug)]
pub struct NullStreamEvent;

impl StreamEventParsed for NullStreamEvent {
    type HttpReducer = NullReducer;
    type GrpcReducer = NullReducer;
    type WsReducer = NullReducer;

    type HttpState = NullState;
    type GrpcState = NullState;
    type WsState = NullState;
}

#[derive(Debug, Clone)]
pub enum StreamEventUnion<Parsed> {
    RawHttp(StreamEventRaw<HttpEvent>),
    RawGrpc(StreamEventRaw<GrpcEvent>),
    RawWs(StreamEventRaw<WebSocketEvent>),
    Parsed(Parsed),
}

#[derive(Debug, Clone)]
pub struct StreamEvent<Parsed> {
    pub stream: SharedStr,
    pub union: StreamEventUnion<Parsed>,
}
