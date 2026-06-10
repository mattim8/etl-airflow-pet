CREATE DATABASE events;

\connect events;

CREATE TABLE IF NOT EXISTS src_events (
    event_id BIGINT PRIMARY KEY,
    event_ts TIMESTAMP NOT NULL,
    payload TEXT NOT NULL,
    updated_ts TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS clients (
    client_id BIGINT PRIMARY KEY,
    client_score NUMERIC(10, 2) NOT NULL,
    time_zone TEXT NOT NULL
);

INSERT INTO src_events (event_id, event_ts, payload, updated_ts) VALUES
    (1, '2026-01-01 09:00:00', '{"type":"signup"}', '2026-01-01 09:05:00'),
    (2, '2026-01-01 09:10:00', '{"type":"purchase"}', '2026-01-01 09:15:00'),
    (3, '2026-01-01 09:20:00', '{"type":"refund"}', '2026-01-01 09:20:00'),
    (4, '2026-01-01 09:25:00', '{"type":"purchase"}', '2026-01-01 09:20:00'),
    (5, '2026-01-01 09:30:00', '{"type":"support"}', '2026-01-01 09:40:00')
ON CONFLICT (event_id) DO UPDATE SET
    event_ts = EXCLUDED.event_ts,
    payload = EXCLUDED.payload,
    updated_ts = EXCLUDED.updated_ts;

INSERT INTO clients (client_id, client_score, time_zone) VALUES
    (101, 99.50, 'Europe/Moscow'),
    (102, 95.00, 'Europe/Moscow'),
    (103, 95.00, 'Asia/Yekaterinburg'),
    (104, 90.25, 'Europe/Samara'),
    (105, 88.00, 'Asia/Novosibirsk'),
    (106, 81.30, 'Europe/Moscow'),
    (107, 75.00, 'Asia/Yekaterinburg'),
    (108, 72.10, 'Europe/Samara')
ON CONFLICT (client_id) DO UPDATE SET
    client_score = EXCLUDED.client_score,
    time_zone = EXCLUDED.time_zone;
