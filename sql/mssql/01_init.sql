IF DB_ID('events_dw') IS NULL
BEGIN
    CREATE DATABASE events_dw;
END;
GO

USE events_dw;
GO

IF OBJECT_ID('dbo.dst_events', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.dst_events (
        event_id BIGINT NOT NULL PRIMARY KEY,
        event_ts DATETIME2 NOT NULL,
        payload NVARCHAR(MAX) NOT NULL,
        updated_ts DATETIME2 NOT NULL
    );
END;
GO