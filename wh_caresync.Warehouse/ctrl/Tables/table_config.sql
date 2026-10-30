CREATE TABLE [ctrl].[table_config] (
    [table_id]           BIGINT        IDENTITY NOT NULL,
    [source_system]      VARCHAR (50)  NOT NULL,
    [source_schema_name] VARCHAR (100) NULL,
    [source_table_name]  VARCHAR (200) NULL,
    [source_file_format] VARCHAR (20)  NULL,
    [source_path]        VARCHAR (500) NULL,
    [bronze_table_name]  VARCHAR (200) NULL,
    [silver_table_name]  VARCHAR (200) NULL,
    [gold_table_name]    VARCHAR (200) NULL,
    [load_type]          VARCHAR (20)  NOT NULL,
    [watermark_column]   VARCHAR (100) NULL,
    [merge_keys]         VARCHAR (200) NULL,
    [stage]              VARCHAR (20)  NOT NULL,
    [is_active]          BIT           NOT NULL,
    [created_timestamp]  DATETIME2 (6) NOT NULL,
    [updated_timestamp]  DATETIME2 (6) NOT NULL
);


GO