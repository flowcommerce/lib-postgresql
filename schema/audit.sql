create schema audit;
set search_path to audit;

CREATE OR REPLACE FUNCTION setup(
  p_schema_name text,
  p_table_name text,
  p_journal_schema_name text DEFAULT 'journal',
  p_journal_table_name text DEFAULT null
) RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
  execute 'alter table ' || p_schema_name || '.' || p_table_name || ' add updated_by_user_id text';
  execute 'alter table ' || p_schema_name || '.' || p_table_name || ' add deleted_at timestamptz';
  perform journal.refresh_journaling(p_schema_name, p_table_name, p_journal_schema_name, coalesce(p_journal_table_name, p_table_name));
end;
$$;
