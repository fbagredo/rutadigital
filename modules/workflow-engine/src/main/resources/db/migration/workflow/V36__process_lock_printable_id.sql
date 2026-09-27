-- process_lock.id is now <name length>:<lockName>:<lockKey> (LockRowId) instead of
-- lockName + NUL + lockKey. PostgreSQL rejects NUL in any text value, so the old encoding could never
-- be inserted there and no PostgreSQL database holds a row in it; JdbcLockService rewrites any such row
-- H2 kept before its first lock operation, also for the ddl-auto deployments this migration never reaches.
--
-- The length prefix and the second separator make the longest id 3 + 1 + 255 + 1 + 255 = 515, past
-- the old 512. Widening a varchar is a metadata-only change in PostgreSQL.
ALTER TABLE process_lock ALTER COLUMN id TYPE VARCHAR(520);
