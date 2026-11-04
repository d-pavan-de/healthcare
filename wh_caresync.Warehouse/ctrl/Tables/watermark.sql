CREATE TABLE [ctrl].[watermark] (
    [table_id]               BIGINT        NOT NULL,
    [watermark_column]       VARCHAR (100) NULL,
    [merge_keys]             VARCHAR (200) NULL,
    [watermark_value]        VARCHAR (200) NULL,
    [last_updated_timestamp] DATETIME2 (6) NOT NULL
);


GO