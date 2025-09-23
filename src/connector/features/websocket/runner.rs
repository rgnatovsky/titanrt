use super::{
    action::{WsAction, WsConnectConfig, WsSendPayload},
    descriptor::WsStreamingDescriptor,
    event::WsEvent,
};
use crate::connector::features::shared::actions::StreamAction;
use crate::connector::features::shared::events::StreamEvent;
use crate::connector::hook::Hook;
use crate::connector::{
    HookArgs, IntoHook, RuntimeCtx, Stream, StreamRunner, StreamSpawner, errors::StreamError,
};
use crate::io::ringbuffer::{RingReceiver, RingSender};
use crate::prelude::{BaseRx, TxPairExt};
use crate::utils::{Reducer, StateMarker, NullState};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::TcpStream;
use std::time::{Duration, Instant};
use tungstenite::{Message as WsMsg, WebSocket, connect, stream::MaybeTlsStream};
use url::Url;

type ActionTx = RingSender<StreamAction<WsAction>>; // канал для действий (что прислал пользователь).
type EventRx = RingReceiver<StreamEvent<WsEvent>>; // канал для событий (что мы эмитим наружу).
pub type WsStream = Stream<ActionTx, EventRx, NullState>; // конкретный тип стрима (action_tx, event_rx, без состояния).

pub struct WsConnectorRunner; // Маркер — сам раннер.

// живое подключение
pub struct Conn {
    ws: WebSocket<MaybeTlsStream<TcpStream>>, // сам вебсокет
    last_ping: Instant, // время последнего пинга
    ping_interval: Option<Duration>, // интервал пинга
}

impl Conn {
    fn new(cfg: &WsConnectConfig) -> anyhow::Result<Self> {
        // Разбираем URL и делаем websocket-connect.
        let url = Url::parse(&cfg.url)?;
        let (ws, _resp) = connect(url)?;

        Ok(Self {
            ws,
            last_ping: Instant::now(),
            ping_interval: cfg.ping_every_secs.map(Duration::from_secs), // настраиваем пинг по конфигу
        })
    }
}

// Spawner — просто маркер
impl<E, R, S> StreamSpawner<WsStreamingDescriptor, E, R, S> for super::connector::WsConnector
where
    E: TxPairExt,
    S: StateMarker,
    R: Reducer,
{
}

// раннер для дескриптора WsStreamingDescriptor, события — WsEvent.
impl<E, R, S> StreamRunner<WsStreamingDescriptor, E, R, S> for super::connector::WsConnector
where
    E: TxPairExt,
    S: StateMarker,
    R: Reducer,
{
    type Config = ();
    type ActionTx = RingSender<StreamAction<WsAction>>;
    type RawEvent = StreamEvent<WsEvent>;
    type HookResult = ();

    fn build_config(&mut self, _desc: &WsStreamingDescriptor) -> anyhow::Result<Self::Config> {
        Ok(())
    }
    // запускает рабочий цикл
    fn run<H>(
        mut ctx: RuntimeCtx<(), WsStreamingDescriptor, RingSender<StreamAction<WsAction>>, E, R, S>,
        hook: H,
    ) -> Result<(), StreamError>
    where
        H: IntoHook<StreamEvent<WsEvent>, E, R, S, WsStreamingDescriptor, ()>,
        E: TxPairExt,
        S: StateMarker,
    {
        let mut hook = hook.into_hook(); // превращаем hook в реальный объект
        let idle_sleep = Duration::from_micros(ctx.desc.idle_sleep_us); // достаём задержку сна между итерациями.

        // Храним все активные подключения в мапе (conn_id → Conn).
        let mut conns: HashMap<usize, Conn> = HashMap::new();

        ctx.health.up(); // Помечаем стрим как живой.

        // Основной цикл
        'outer: loop {
            if ctx.cancel.is_cancelled() {
                ctx.health.down();
                // Если пришёл сигнал отмены → выключаем health, закрываем все подключения, выходим.
                for (_, mut c) in conns.drain() {
                    let _ = c.ws.close(None);
                }
                break 'outer;
            }

            // обработка входящих Action’ов
            while let Ok(action) = ctx.action_rx.try_recv() {
                let (inner, conn_id, _req_id, _label, _timeout, _rl_ctx, _rl_weight, _json) =
                    action.into_parts();
                    // Проверяем очередь действий.
                let Some(conn_id) = conn_id else {
                    // Если conn_id нет → шлём ошибку.
                    let ev = StreamEvent::new(WsEvent::from_error("ws action missing conn_id".to_string()));
                    hook.call(HookArgs::new(
                        ev,
                        &mut ctx.event_tx,
                        &mut ctx.reducer,
                        &ctx.state,
                        &ctx.desc,
                        &mut ctx.health,
                    ));
                    continue;
                };

                let Some(inner) = inner else {
                    continue;
                };

                // Три действия - Connect, Send, Disconnect
                // Connect — создаём новое подключение, сохраняем в мапе, эмитим connected().
                // Send — если подключение есть, шлём payload. Если нет — ошибка.
                // Disconnect — закрываем подключение, удаляем из мапы, эмитим disconnected().
                match inner {
                    WsAction::Connect(cfg) => {
                        // если был — закрыть и заменить
                        if let Some(mut old) = conns.remove(&conn_id) {
                            let _ = old.ws.close(None);
                        }
                        match Conn::new(&cfg) {
                            Ok(c) => {
                                conns.insert(conn_id, c);
                                let ev = StreamEvent::new(WsEvent::connected());
                                hook.call(HookArgs::new(
                                    ev,
                                    &mut ctx.event_tx,
                                    &mut ctx.reducer,
                                    &ctx.state,
                                    &ctx.desc,
                                    &mut ctx.health,
                                ));
                            }
                            Err(e) => {
                                let ev = StreamEvent::new(WsEvent::from_error(format!(
                                    "connect failed: {e}"
                                )));
                                hook.call(HookArgs::new(
                                    ev,
                                    &mut ctx.event_tx,
                                    &mut ctx.reducer,
                                    &ctx.state,
                                    &ctx.desc,
                                    &mut ctx.health,
                                ));
                            }
                        }
                    }
                    WsAction::Send(payload) => {
                        if let Some(conn) = conns.get_mut(&conn_id) {
                            let res = match payload {
                                WsSendPayload::Text(s) => conn.ws.send(WsMsg::Text(s)),
                                WsSendPayload::Binary(b) => conn.ws.send(WsMsg::Binary(b.to_vec())),
                            };
                            if let Err(e) = res {
                                let ev =
                                    StreamEvent::new(WsEvent::from_error(format!("send error: {e}")));
                                hook.call(HookArgs::new(
                                    ev,
                                    &mut ctx.event_tx,
                                    &mut ctx.reducer,
                                    &ctx.state,
                                    &ctx.desc,
                                    &mut ctx.health,
                                ));
                            }
                        } else {
                            let ev =
                                StreamEvent::new(WsEvent::from_error("send to unknown conn_id".to_string()));
                            hook.call(HookArgs::new(
                                ev,
                                &mut ctx.event_tx,
                                &mut ctx.reducer,
                                &ctx.state,
                                &ctx.desc,
                                &mut ctx.health,
                            ));
                        }
                    }
                    WsAction::Disconnect => {
                        if let Some(mut conn) = conns.remove(&conn_id) {
                            let _ = conn.ws.close(None);
                            let ev = StreamEvent::new(WsEvent::disconnected(
                                None,
                                Some("client disconnect".into()),
                            ));
                            hook.call(HookArgs::new(
                                ev,
                                &mut ctx.event_tx,
                                &mut ctx.reducer,
                                &ctx.state,
                                &ctx.desc,
                                &mut ctx.health,
                            ));
                        }
                    }
                }
            }

            // читаем данные со всех сокетов (итеративно, короткие таймауты)
            let mut to_drop = Vec::new();

            for (&id, conn) in conns.iter_mut() {
                // Для каждого соединения: если пора → слать Ping.
                if let Some(every) = conn.ping_interval {
                    if conn.last_ping.elapsed() >= every {
                        let _ = conn.ws.send(WsMsg::Ping(vec![]));
                        conn.last_ping = Instant::now();
                    }
                }
                // Мы читаем сообщение, конвертим его в WsEvent, пушим в hook.
                match conn.ws.read() {
                    Ok(WsMsg::Text(s)) => {
                        let ev = StreamEvent::new(WsEvent::from_text(s));
                        hook.call(HookArgs::new(
                            ev,
                            &mut ctx.event_tx,
                            &mut ctx.reducer,
                            &ctx.state,
                            &ctx.desc,
                            &mut ctx.health,
                        ));
                    }
                    Ok(WsMsg::Binary(b)) => {
                        let ev = StreamEvent::new(WsEvent::from_binary(b));
                        hook.call(HookArgs::new(
                            ev,
                            &mut ctx.event_tx,
                            &mut ctx.reducer,
                            &ctx.state,
                            &ctx.desc,
                            &mut ctx.health,
                        ));
                    }
                    Ok(WsMsg::Pong(_)) => {
                        // pong не поднимаем наружу — это транспортная рутина
                    }
                    Ok(WsMsg::Ping(_)) => {
                        let _ = conn.ws.send(WsMsg::Ping(vec![]));
                    }
                    Ok(WsMsg::Close(frame)) => {
                        let (code, reason) = match frame {
                            Some(f) => (Some(u16::from(f.code)), Some(f.reason.to_string())),
                            None => (None, None),
                        };
                        let ev = StreamEvent::new(WsEvent::disconnected(code, reason));
                        hook.call(HookArgs::new(
                            ev,
                            &mut ctx.event_tx,
                            &mut ctx.reducer,
                            &ctx.state,
                            &ctx.desc,
                            &mut ctx.health,
                        ));
                        to_drop.push(id);
                    }
                    Ok(_) => {
                        // на всякий: игнорируем прочие Message-варианты (если появятся)
                    }
                    Err(tungstenite::Error::Io(ref e))
                        if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut =>
                    {
                        // просто нет данных — норм
                    }
                    Err(e) => {
                        let ev = StreamEvent::new(WsEvent::from_error(format!("read error: {e}")));
                        hook.call(HookArgs::new(
                            ev,
                            &mut ctx.event_tx,
                            &mut ctx.reducer,
                            &ctx.state,
                            &ctx.desc,
                            &mut ctx.health,
                        ));
                        to_drop.push(id);
                    }
                }
            }

            // Если соединение умерло → добавляем в to_drop.
            // Удаляем мёртвые подключения.
            for id in to_drop {
                conns.remove(&id);
            }
            // Чтобы не жечь CPU, делаем микросон.
            if ctx.desc.idle_sleep_us > 0 {
                std::thread::sleep(idle_sleep);
            }
        }

        Ok(())
    }
}
