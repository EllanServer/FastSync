-- ============================================================
-- FastSync Greenfield v2 Deployment: Drop Old Tables
-- ============================================================
-- Run this BEFORE starting FastSync v2 for the first time.
-- FastSync v2 uses (cluster_id, uuid) primary keys and a new
-- schema_version table. Old v1 tables (without cluster_id) will
-- cause the plugin to refuse startup.
--
-- Usage:
--   mysql -u <user> -p <database> < scripts/drop_fastsync_tables.sql
--
-- WARNING: This permanently deletes ALL FastSync player data.
-- Back up first if you need to preserve any data.
-- ============================================================

-- Replace fastsync_ with your table-prefix if customized.
DROP TABLE IF EXISTS `fastsync_player_component`;
DROP TABLE IF EXISTS `fastsync_player_data`;
DROP TABLE IF EXISTS `fastsync_snapshots`;
DROP TABLE IF EXISTS `fastsync_schema_version`;
