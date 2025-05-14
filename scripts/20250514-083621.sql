DROP FUNCTION IF EXISTS journal.refresh_journaling;
ALTER FUNCTION journal.refresh_journaling_native RENAME TO refresh_journaling;
