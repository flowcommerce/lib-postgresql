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
declare
  v_journal_table_name text;
begin
  v_journal_table_name = coalesce(p_journal_table_name, p_table_name);

  execute 'alter table ' || p_schema_name || '.' || p_table_name || ' add created_at timestamptz default now() not null';
  execute 'alter table ' || p_schema_name || '.' || p_table_name || ' add updated_by_user_id text not null';

  -- add journaling to this table
  perform journal.refresh_journaling(p_schema_name, p_table_name, p_journal_schema_name, v_journal_table_name);

  -- add partition management to journal table
  -- this will create 1 current, 4 past, and 4 future monthly time-based partitions for journal.table_name
  perform partman.create_parent(p_journal_schema_name || '.' || v_journal_table_name, 'journal_timestamp', 'time', 'monthly');
end;
$$;
