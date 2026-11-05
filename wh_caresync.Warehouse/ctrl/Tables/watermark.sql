CREATE TABLE [ctrl].[watermark] (
    [table_id]               BIGINT        NOT NULL,
    [watermark_column]       VARCHAR (100) NULL,
    [watermark_value]        DATETIME2 (6) NULL,
    [last_updated_timestamp] DATETIME2 (6) NOT NULL
);


GO