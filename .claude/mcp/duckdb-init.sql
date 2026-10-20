-- Startup SQL for the "duckdb" MCP server (mcp-server-motherduck --init-sql).
-- Loads the Delta and S3 readers and creates one S3 secret pointing at the
-- local MinIO. Credentials come from the environment set in .mcp.json
-- (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY = the MinIO `spark` account,
-- defaults from .env.example), read through the `aws` credential chain.
INSTALL delta;
LOAD delta;
INSTALL httpfs;
LOAD httpfs;
INSTALL aws;
LOAD aws;
CREATE OR REPLACE SECRET minio (
    TYPE s3,
    PROVIDER credential_chain,
    CHAIN 'env',
    ENDPOINT 'localhost:9000',
    URL_STYLE 'path',
    USE_SSL false,
    REGION 'us-east-1'
);
