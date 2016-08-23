create schema queue;
set search_path to queue;

CREATE OR REPLACE FUNCTION create_queue(
  p_schema_name text,
  p_table_name text,
  p_queue_schema_name text DEFAULT 'queue',
  p_queue_table_name text DEFAULT null
) RETURNS text
    LANGUAGE plpgsql
    AS $$
declare
  v_queue_table_name text;
  v_source_name text;
  v_procedure_name text;
  v_trigger_name text;
  v_sql text;
begin
  v_queue_table_name = p_queue_schema_name || '.' || coalesce(p_queue_table_name, p_table_name);
  v_source_name = p_schema_name || '.' || p_table_name;

  v_sql = 'create table ' || v_queue_table_name || '(journal_id bigint primary key, created_at timestamptz default now() not null, processed_at timestamptz)';
  execute v_sql;

  v_sql = 'create index on ' || v_queue_table_name || '(journal_id) where processed_at is null';
  execute v_sql;


  v_procedure_name = p_queue_schema_name || '.' || p_table_name || '_queue_insert';
  v_trigger_name = p_table_name || '_queue_insert_trigger';

  v_sql = 'create or replace function ' || v_procedure_name || '() returns trigger language plpgsql as ''';
  v_sql := v_sql || ' begin ';
  v_sql := v_sql || '  insert into ' || v_queue_table_name || ' (journal_id) values (new.journal_id);';
  v_sql := v_sql || ' return new; end; ''';
  execute v_sql;

  -- create the trigger
  v_sql = 'drop trigger if exists ' || v_trigger_name || ' on ' || v_source_name || '; ' ||
          'create trigger ' || v_trigger_name || ' after insert on ' || v_source_name ||
          ' for each row execute procedure ' || v_procedure_name || '()';

  execute v_sql;


  v_procedure_name = p_queue_schema_name || '.' || p_table_name || '_queue_update';
  v_trigger_name = p_table_name || '_queue_update_trigger';

  v_sql = 'create or replace function ' || v_procedure_name || '() returns trigger language plpgsql as ''';
  v_sql := v_sql || ' begin ';
  v_sql := v_sql || '  if new.journal_id != old.journal_id then ';
  v_sql := v_sql || '    raise ''''The table ' || v_source_name || ' has a queue table - updated to journal_id are not supported'''';';
  v_sql := v_sql || '  end if;';
  v_sql := v_sql || '  insert into ' || v_queue_table_name || ' (journal_id) values (new.journal_id);';
  v_sql := v_sql || ' return new; end; ''';
  execute v_sql;

  -- create the trigger
  v_sql = 'drop trigger if exists ' || v_trigger_name || ' on ' || v_source_name || '; ' ||
          'create trigger ' || v_trigger_name || ' after update on ' || v_source_name ||
          ' for each row execute procedure ' || v_procedure_name || '()';

  execute v_sql;


  v_procedure_name = p_queue_schema_name || '.' || p_table_name || '_queue_delete';
  v_trigger_name = p_table_name || '_queue_delete_trigger';

  v_sql = 'create or replace function ' || v_procedure_name || '() returns trigger language plpgsql as ''';
  v_sql := v_sql || ' begin ';
  v_sql := v_sql || '  insert into ' || v_queue_table_name || ' (journal_id) values (old.journal_id);';
  v_sql := v_sql || ' return old; end; ''';
  execute v_sql;

  -- create the trigger
  v_sql = 'drop trigger if exists ' || v_trigger_name || ' on ' || v_source_name || '; ' ||
          'create trigger ' || v_trigger_name || ' after delete on ' || v_source_name ||
          ' for each row execute procedure ' || v_procedure_name || '()';

  execute v_sql;

  return v_queue_table_name;
end;
$$;
