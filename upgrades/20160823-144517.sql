create or replace function journal.refresh_journaling(
  p_source_schema_name in varchar, p_source_table_name in varchar,
  p_target_schema_name in varchar, p_target_table_name in varchar
) returns varchar language plpgsql as $$
declare
  row record;
  v_journal_name text;
  v_data_type character varying;
begin
  v_journal_name = p_target_schema_name || '.' || p_target_table_name;
  if exists(select 1 from information_schema.tables where table_schema = p_target_schema_name and table_name = p_target_table_name) then
    for row in (select column_name, journal.get_data_type_string(information_schema.columns.*) as data_type from information_schema.columns where table_schema = p_source_schema_name and table_name = p_source_table_name order by ordinal_position) loop

      -- NB: Specifically choosing to not drop deleted columns from the journal table, to preserve the data.
      -- There are no constraints (other than not null on primary key columns) on the journaling table
      -- columns anyway, so leaving it populated with null will be fine.
      select journal.get_data_type_string(information_schema.columns.*) into v_data_type from information_schema.columns where table_schema = p_target_schema_name and table_name = p_target_table_name and column_name = row.column_name;
      if not found then
        execute 'alter table ' || v_journal_name || ' add ' || row.column_name || ' ' || row.data_type;
      elsif (row.data_type != v_data_type) then
        execute 'alter table ' || v_journal_name || ' alter column ' || row.column_name || ' type ' || row.data_type;
      end if;

    end loop;
  else
    execute 'create table ' || v_journal_name || ' as select * from ' || p_source_schema_name || '.' || p_source_table_name || ' limit 0';
    execute 'alter table ' || v_journal_name || ' add journal_timestamp timestamp with time zone not null default now() ';
    execute 'alter table ' || v_journal_name || ' add journal_operation text not null ';
    execute 'alter table ' || v_journal_name || ' add journal_id bigserial primary key ';
    execute 'alter table ' || v_journal_name || ' set (fillfactor=100) ';
    execute 'comment on table ' || v_journal_name || ' is ''Created by plsql function refresh_journaling to shadow all inserts and updates on the table ' || p_source_schema_name || '.' || p_source_table_name || '''';
    perform journal.add_primary_key_data(p_source_schema_name, p_source_table_name, p_target_schema_name, p_target_table_name);
    perform journal.create_prevent_update_trigger(p_target_schema_name, p_target_table_name);
    perform journal.create_prevent_delete_trigger(p_target_schema_name, p_target_table_name);
  end if;

  perform journal.refresh_journal_trigger(p_source_schema_name, p_source_table_name, p_target_schema_name, p_target_table_name);

  return v_journal_name;

end;
$$;

create or replace function journal.create_prevent_update_trigger(p_schema_name character varying, p_table_name character varying) returns character varying
  language plpgsql
  as $$
declare
  v_name varchar;
begin
  v_name = p_table_name || '_prevent_update_trigger';
  execute 'create trigger ' || v_name || ' before update on ' || p_schema_name || '.' || p_table_name || ' for each row execute procedure journal.prevent_update()';
  return v_name;
end;
$$;


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

  v_sql = 'create table ' || v_queue_table_name || '(journal_id bigint primary key, created_at timestamptz default now() not null, processed_at timestamptz, error text)';
  execute v_sql;

  v_sql = 'create index on ' || v_queue_table_name || '(journal_id) where processed_at is null';
  execute v_sql;


  perform partman.create_parent(v_queue_table_name, 'created_at', 'time', 'daily');
  update partman.part_config
     set retention = '1 week',
         retention_keep_table = false,
         retention_keep_index = false
   where parent_table in (v_queue_table_name);

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



create or replace function upgrade_journal() returns integer language plpgsql as $$
declare
  row record;
  count integer = 0;
begin
  for row in (select table_name from information_schema.tables where table_schema='journal') loop
    execute 'ALTER TABLE journal.' || row.table_name || ' SET ( FILLFACTOR = 100 )';
    count = count + 1;
  end loop;
  return count;
end;
$$;

select upgrade_journal();

drop function upgrade_journal();
