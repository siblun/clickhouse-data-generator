CREATE TABLE users (
    id UInt64,
    mtime DateTime64(9, 'UTC'),
    name String,
    age UInt16,
    is_active Bool,
    created_at DateTime
)
ENGINE = MergeTree
ORDER BY id;