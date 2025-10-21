import { useCallback, useEffect, useMemo, useState } from "react";
import type { ChangeEvent } from "react";
import { meshpipeClient } from "../../lib/grpc/client";
import { useLiveQuery } from "../../lib/state/live";
import { LiveStatusBadge } from "../../lib/state/LiveStatusBadge";
import type {
  ChatMessage,
  GetChatWindowResponse,
  ListNodeNamesResponse,
  ListPrimaryChannelsResponse,
} from "../../gen/meshpipe/v1/data";
import "./ChatPage.css";

const CHAT_PAGE_SIZE = 120;

export function ChatPage() {
  const [channels, setChannels] = useState<string[]>([]);
  const [channelId, setChannelId] = useState<string>("");
  const [windowHours, setWindowHours] = useState<number>(6);
  const [search, setSearch] = useState<string>("");
  const [nodeNames, setNodeNames] = useState<Record<number, string>>({});

  useEffect(() => {
    let cancelled = false;
    meshpipeClient
      .ListPrimaryChannels({})
      .then((response: ListPrimaryChannelsResponse) => {
        if (!cancelled) {
          setChannels(response.channels ?? []);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setChannels([]);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const key = useMemo(() => `chat:${channelId || "all"}:${windowHours}`, [channelId, windowHours]);

  const fetcher = useCallback(
    () =>
      meshpipeClient.GetChatWindow({
        filter: channelId ? { channelId } : undefined,
        windowHours,
        pagination: { pageSize: CHAT_PAGE_SIZE, cursor: "" },
      }),
    [channelId, windowHours],
  );

  const state = useLiveQuery<GetChatWindowResponse>(key, fetcher, { intervalMs: 10000 });

  useEffect(() => {
    const messages = state.data?.messages ?? [];
    const missing = collectUnknownNodeIds(messages, nodeNames);
    if (!missing.length) {
      return;
    }
    let cancelled = false;
    meshpipeClient
      .ListNodeNames({ nodeIds: missing })
      .then((resp: ListNodeNamesResponse) => {
        if (cancelled) {
          return;
        }
        setNodeNames((prev) => {
          const next = { ...prev };
          for (const entry of resp.entries ?? []) {
            if (entry.nodeId !== 0) {
              next[entry.nodeId] = entry.displayName || entry.shortName || `Node ${entry.nodeId}`;
            }
          }
          return next;
        });
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
  }, [state.data, nodeNames]);

  const filteredMessages = useMemo(() => {
    const messages = state.data?.messages ?? [];
    const sorted = [...messages].sort((a, b) => {
      const tsA = a.timestamp?.getTime() ?? 0;
      const tsB = b.timestamp?.getTime() ?? 0;
      return tsB - tsA;
    });
    if (!search.trim()) {
      return sorted;
    }
    const query = search.trim().toLowerCase();
    return sorted.filter((message) => (message.text ?? "").toLowerCase().includes(query));
  }, [state.data?.messages, search]);

  const showLoader = state.status === "idle" || (state.status === "loading" && !state.data);

  const summary = state.data?.counters;

  const handleChannelChange = (event: ChangeEvent<HTMLSelectElement>) => {
    setChannelId(event.target.value);
  };

  const handleWindowChange = (event: ChangeEvent<HTMLSelectElement>) => {
    setWindowHours(Number(event.target.value));
  };

  const handleSearchChange = (event: ChangeEvent<HTMLInputElement>) => {
    setSearch(event.target.value);
  };

  return (
    <div className="page">
      <header className="page__header">
        <div>
          <h1>Чат Meshpipe</h1>
          <p>Последние сообщения Meshworks с возможностью фильтровать по каналу и окну времени.</p>
        </div>
        {summary ? (
          <div className="page__metadata">
            <span>1ч: {summary.messages1h.toLocaleString()}</span>
            <span>6ч: {summary.messages6h.toLocaleString()}</span>
            <span>24ч: {summary.messages24h.toLocaleString()}</span>
          </div>
        ) : null}
      </header>

      <LiveStatusBadge state={state} className="page__refresh-meta" />

      <section className="chat-controls" aria-label="Фильтры чата">
        <label className="chat-controls__field">
          <span>Канал</span>
          <select value={channelId} onChange={handleChannelChange}>
            <option value="">Все каналы</option>
            {channels.map((channel) => (
              <option key={channel} value={channel}>
                {channel}
              </option>
            ))}
          </select>
        </label>
        <label className="chat-controls__field">
          <span>Окно</span>
          <select value={windowHours} onChange={handleWindowChange}>
            {[1, 3, 6, 12, 24, 48].map((hours) => (
              <option key={hours} value={hours}>
                {hours} ч
              </option>
            ))}
          </select>
        </label>
        <label className="chat-controls__field chat-controls__field--search">
          <span>Поиск по тексту</span>
          <input
            type="search"
            placeholder="Введите часть сообщения"
            value={search}
            onChange={handleSearchChange}
          />
        </label>
      </section>

      {showLoader ? (
        <div className="loading">
          <div className="spinner" />
          <p>Загружаем сообщения…</p>
        </div>
      ) : null}

      {state.status === "error" && state.error ? (
        <div className="error-banner">
          <strong>Ошибка:</strong> {state.error}
        </div>
      ) : null}

      <section className="chat-list" aria-live="polite">
        {filteredMessages.length === 0 && !showLoader ? (
          <p className="chat-list__empty">Сообщений не найдено.</p>
        ) : null}

        {filteredMessages.map((message) => (
          <article key={message.packetId} className="chat-message">
            <header className="chat-message__header">
              <div>
                <span className="chat-message__timestamp">
                  {message.timestamp ? message.timestamp.toLocaleString() : "—"}
                </span>
                <span className="chat-message__channel">#{message.channelId || "—"}</span>
              </div>
              <div className="chat-message__nodes">
                <span>{resolveNodeName(message.fromNodeId, nodeNames)}</span>
                <span className="chat-message__arrow">→</span>
                <span>{resolveNodeName(message.toNodeId, nodeNames)}</span>
              </div>
            </header>
            <p className="chat-message__text">{message.text || "(empty)"}</p>
            {message.gateways && message.gateways.length ? (
              <footer className="chat-message__footer">
                {message.gateways.map((gw) => (
                  <span key={gw.gatewayId} className="chat-message__gateway">
                    {gw.gatewayId}
                    {Number.isFinite(gw.rssi) ? ` · RSSI ${gw.rssi} dБм` : ""}
                    {Number.isFinite(gw.snr) ? ` · SNR ${gw.snr.toFixed(1)} дБ` : ""}
                  </span>
                ))}
              </footer>
            ) : null}
          </article>
        ))}
      </section>
    </div>
  );
}

function collectUnknownNodeIds(messages: ChatMessage[], known: Record<number, string>): number[] {
  const ids = new Set<number>();
  for (const message of messages) {
    if (message.fromNodeId && !(message.fromNodeId in known)) {
      ids.add(message.fromNodeId);
    }
    if (message.toNodeId && !(message.toNodeId in known)) {
      ids.add(message.toNodeId);
    }
  }
  return Array.from(ids);
}

function resolveNodeName(nodeId: number, known: Record<number, string>): string {
  if (nodeId === 0) {
    return "Broadcast";
  }
  return known[nodeId] ?? `Node ${nodeId}`;
}
