CREATE SCHEMA IF NOT EXISTS journal;

-- DETAILS
CREATE OR REPLACE FUNCTION journal.diff_details(
  p_table_name TEXT,
  p_ids ANYARRAY,
  p_columns_to_compare TEXT[] DEFAULT NULL
) RETURNS TABLE (id TEXT, diff TEXT) AS $$
DECLARE
  schema_name TEXT;
  table_name_var TEXT;
  column_names TEXT[];
  from_column_names TEXT;
  diff_columns TEXT;
  query TEXT;
BEGIN
  -- Extract schema and table name
  IF strpos(p_table_name, '.') > 0 THEN
      schema_name := split_part(p_table_name, '.', 1);
      table_name_var := split_part(p_table_name, '.', 2);
  ELSE
      schema_name := 'public';
      table_name_var := p_table_name;
  END IF;

  -- Check that the table exists and contains both journal_id and journal_timestamp columns
  IF NOT EXISTS (
      SELECT 1
      FROM information_schema.columns c
      WHERE c.table_schema = schema_name
      AND c.table_name = table_name_var
      AND c.column_name IN ('journal_id', 'journal_timestamp')
      GROUP BY c.table_schema, c.table_name
      HAVING COUNT(DISTINCT c.column_name) = 2
  ) THEN
      RAISE EXCEPTION 'Table %I.%I does not exist or does not contain both journal_id and journal_timestamp columns', schema_name, table_name_var;
  END IF;

  -- Compute the list of columns to compare, keeping only the columns that are passed in if specified
  SELECT array_agg(c.column_name::TEXT)
  INTO column_names
  FROM information_schema.columns c
  WHERE c.table_schema = schema_name
  AND c.table_name = table_name_var
  AND c.column_name NOT IN ('id', 'journal_id', 'journal_timestamp', 'journal_operation', '_updated_at', 'updated_at', 'version', '_version', 'hash_code', '_hash_code')
  AND (p_columns_to_compare IS NULL OR c.column_name = ANY(p_columns_to_compare));

  -- Generate the alias list for the hydrated CTE
  SELECT string_agg(
    format(
      CASE
        WHEN c.data_type IN ('json', 'jsonb') THEN 'o.%I::TEXT AS from_%I, j.%I::TEXT'
        ELSE 'o.%I AS from_%I, j.%I'
      END,
      c.column_name, c.column_name, c.column_name
    ), ', '
  )
  INTO from_column_names
  FROM information_schema.columns c
  WHERE c.table_schema = schema_name
  AND c.table_name = table_name_var
  AND c.column_name = ANY(column_names);

  -- Generate the diff logic for JSONB construction with specific formatting for journal fields
  SELECT string_agg(
    format(
      'jsonb_build_object(''%I'', CASE WHEN from_%I IS DISTINCT FROM %I THEN COALESCE(from_%I::TEXT, ''<null>'') || '' -> '' || COALESCE(%I::TEXT, ''<null>'') ELSE NULL END)',
      c.column_name, c.column_name, c.column_name, c.column_name, c.column_name
    ), ' || '
  )
  INTO diff_columns
  FROM information_schema.columns c
  WHERE c.table_schema = schema_name
  AND c.table_name = table_name_var
  AND c.column_name = ANY(column_names);

  -- Construct the dynamic SQL query
  query := format('
    WITH ids AS (
      SELECT DISTINCT id
      FROM %I.%I
      WHERE id = ANY(%L)
    ), journals AS (
      SELECT
        id, journal_id, journal_timestamp, journal_operation,
        %s,
        lag(journal_id) OVER (PARTITION BY id ORDER BY journal_id ASC) AS from_journal_id
      FROM %I.%I
      WHERE id IN (SELECT id FROM ids)
    ), hydrated AS (
      SELECT
        j.id AS id,
        o.journal_id as from_journal_id,
        j.journal_id,
        o.journal_timestamp as from_journal_timestamp,
        j.journal_timestamp,
        o.journal_operation as from_journal_operation,
        j.journal_operation,
        %s
      FROM journals j
      JOIN journals o ON j.from_journal_id = o.journal_id
    ), diffs AS (
      SELECT
        id,
        journal_id,
        jsonb_strip_nulls(
          jsonb_build_object(
            ''journal_timestamp'', CASE WHEN from_journal_timestamp IS DISTINCT FROM journal_timestamp THEN COALESCE(from_journal_timestamp::TEXT, ''<null>'') || '' -> '' || COALESCE(journal_timestamp::TEXT, ''<null>'') END,
            ''journal_operation'', CASE WHEN from_journal_operation IS DISTINCT FROM journal_operation THEN COALESCE(from_journal_operation::TEXT, ''<null>'') || '' -> '' || COALESCE(journal_operation::TEXT, ''<null>'') END
          ) || %s
        ) AS diff
      FROM hydrated
      WHERE %s ORDER BY 1, 2 DESC
    )
    SELECT
      id::TEXT,
      jsonb_pretty(jsonb_agg(diff)) AS diff
    FROM diffs
    GROUP BY id
    ORDER BY max(journal_id)
  ',
  schema_name, table_name_var,
  p_ids,
  array_to_string(column_names, ', '), schema_name, table_name_var,
  from_column_names,
  diff_columns,
  (SELECT string_agg(
    format(
      'from_%I IS DISTINCT FROM %I',
      column_name, column_name
    ), ' OR '
  ) FROM unnest(column_names) AS c(column_name))
  );

  -- Execute the dynamic SQL query
  RETURN QUERY EXECUTE query;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION journal.diff_details(
  p_table_name TEXT,
  p_id TEXT,
  p_columns_to_compare TEXT[] DEFAULT NULL
) RETURNS TABLE (id TEXT, diff TEXT) AS $$
-- call the array function
BEGIN
  RETURN QUERY
  SELECT * FROM journal.diff_details(p_table_name, ARRAY[p_id], p_columns_to_compare);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION journal.diff_details(
  p_table_name TEXT,
  p_id BIGINT,
  p_columns_to_compare TEXT[] DEFAULT NULL
) RETURNS TABLE (id TEXT, diff TEXT) AS $$
-- call the array function
BEGIN
  RETURN QUERY
  SELECT * FROM journal.diff_details(p_table_name, ARRAY[p_id], p_columns_to_compare);
END;
$$ LANGUAGE plpgsql;


-- SUMMARY
CREATE OR REPLACE FUNCTION journal.diff_summary(
  p_table_name TEXT,
  p_ids ANYARRAY
) RETURNS TABLE (
    distinct_ids bigint,
    distinct_ids_with_history bigint,
    distinct_journal_ids bigint,
    distinct_journal_ids_with_previous bigint,
    diff_counts TEXT
) AS $$
DECLARE
  schema_name TEXT;
  table_name_var TEXT;
  column_names TEXT;
  from_column_aliases TEXT;
  count_statements TEXT;
  jsonb_concat TEXT;
  query TEXT;
BEGIN
  -- Extract schema and table name
  IF strpos(p_table_name, '.') > 0 THEN
      schema_name := split_part(p_table_name, '.', 1);
      table_name_var := split_part(p_table_name, '.', 2);
  ELSE
      schema_name := 'public';
      table_name_var := p_table_name;
  END IF;

 -- Check that the table exists and contains both journal_id and journal_timestamp columns
  IF NOT EXISTS (
      SELECT 1
      FROM information_schema.columns c
      WHERE c.table_schema = schema_name
      AND c.table_name = table_name_var
      AND c.column_name IN ('journal_id', 'journal_timestamp')
      GROUP BY c.table_schema, c.table_name
      HAVING COUNT(DISTINCT c.column_name) = 2
  ) THEN
      RAISE EXCEPTION 'Table %I.%I does not exist or does not contain both journal_id and journal_timestamp columns', schema_name, table_name_var;
  END IF;

  -- Compute the list of columns to compare, excluding specific columns
  SELECT string_agg(c.column_name::TEXT, ', ')
  INTO column_names
  FROM information_schema.columns c
  WHERE c.table_schema = schema_name
  AND c.table_name = table_name_var
  AND c.column_name NOT IN ('id', 'journal_id', 'journal_timestamp', 'journal_operation', '_updated_at', 'updated_at', 'version', '_version', 'hash_code', '_hash_code');

  -- Generate the alias list for the hydrated CTE
  SELECT string_agg(
    format(
      CASE
        WHEN c.data_type IN ('json', 'jsonb') THEN 'o.%I::TEXT AS from_%I, j.%I::TEXT'
        ELSE 'o.%I AS from_%I, j.%I'
      END,
      c.column_name, c.column_name, c.column_name
    ), ', '
  )
  INTO from_column_aliases
  FROM information_schema.columns c
  WHERE c.table_schema = schema_name
  AND c.table_name = table_name_var
  AND c.column_name NOT IN ('id', 'journal_id', 'journal_timestamp', 'journal_operation', '_updated_at', 'updated_at', 'version', '_version', 'hash_code', '_hash_code');

  -- Generate the count statements for differences dynamically, creating multiple jsonb_build_object calls concatenated with ||
  SELECT string_agg(
    format(
      'jsonb_build_object(''%I'', NULLIF(count(*) filter (where from_journal_id is not null and %I IS DISTINCT FROM from_%I), 0))',
      c.column_name, c.column_name, c.column_name
    ), ' || '
  )
  INTO jsonb_concat
  FROM information_schema.columns c
  WHERE c.table_schema = schema_name
  AND c.table_name = table_name_var
  AND c.column_name NOT IN ('id', 'journal_id', 'journal_timestamp', 'journal_operation', '_updated_at', 'updated_at', 'version', '_version', 'hash_code', '_hash_code');

  -- Construct the dynamic SQL query
  query := format('
    WITH ids AS (
      SELECT DISTINCT id
      FROM %I.%I
      %s
    ), journals AS (
      SELECT
        id, journal_id, journal_timestamp, journal_operation,
        %s,
        lag(journal_id) OVER (PARTITION BY id ORDER BY journal_id ASC) AS from_journal_id
      FROM %I.%I
      WHERE id IN (SELECT id FROM ids)
    ), hydrated AS (
      SELECT
        j.id,
        o.journal_id as from_journal_id,
        j.journal_id,
        o.journal_timestamp as from_journal_timestamp,
        j.journal_timestamp,
        o.journal_operation as from_journal_operation,
        j.journal_operation,
        %s
      FROM journals j
      LEFT JOIN journals o ON j.from_journal_id = o.journal_id
    )
    SELECT
      count(distinct id) as distinct_ids,
      count(distinct id) filter (where from_journal_id is not null) as distinct_ids_with_history,
      count(journal_id) as distinct_journal_ids,
      count(journal_id) filter (where from_journal_id is not null) as distinct_journal_ids_with_previous,
      jsonb_pretty(jsonb_strip_nulls(%s)) as diff_counts
    FROM hydrated
  ',
  schema_name, table_name_var,
  CASE WHEN p_ids IS NOT NULL THEN format('WHERE id = ANY(%L)', p_ids) ELSE '' END,
  column_names, schema_name, table_name_var,
  from_column_aliases,
  jsonb_concat
  );

  -- Execute the dynamic SQL query
  RETURN QUERY EXECUTE query;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION journal.diff_summary(
  p_table_name TEXT
) RETURNS TABLE (
    distinct_ids bigint,
    distinct_ids_with_history bigint,
    distinct_journal_ids bigint,
    distinct_journal_ids_with_previous bigint,
    diff_counts TEXT
) AS $$
DECLARE
  schema_name TEXT;
  table_name_var TEXT;
  column_names TEXT;
  from_column_aliases TEXT;
  count_statements TEXT;
  jsonb_concat TEXT;
  query TEXT;
BEGIN
  -- Extract schema and table name
  IF strpos(p_table_name, '.') > 0 THEN
      schema_name := split_part(p_table_name, '.', 1);
      table_name_var := split_part(p_table_name, '.', 2);
  ELSE
      schema_name := 'public';
      table_name_var := p_table_name;
  END IF;

 -- Check that the table exists and contains both journal_id and journal_timestamp columns
  IF NOT EXISTS (
      SELECT 1
      FROM information_schema.columns c
      WHERE c.table_schema = schema_name
      AND c.table_name = table_name_var
      AND c.column_name IN ('journal_id', 'journal_timestamp')
      GROUP BY c.table_schema, c.table_name
      HAVING COUNT(DISTINCT c.column_name) = 2
  ) THEN
      RAISE EXCEPTION 'Table %I.%I does not exist or does not contain both journal_id and journal_timestamp columns', schema_name, table_name_var;
  END IF;

  -- Compute the list of columns to compare, excluding specific columns
  SELECT string_agg(c.column_name::TEXT, ', ')
  INTO column_names
  FROM information_schema.columns c
  WHERE c.table_schema = schema_name
  AND c.table_name = table_name_var
  AND c.column_name NOT IN ('id', 'journal_id', 'journal_timestamp', 'journal_operation', '_updated_at', 'updated_at', 'version', '_version', 'hash_code', '_hash_code');

  -- Generate the alias list for the hydrated CTE
  SELECT string_agg(
    format(
      CASE
        WHEN c.data_type IN ('json', 'jsonb') THEN 'o.%I::TEXT AS from_%I, j.%I::TEXT'
        ELSE 'o.%I AS from_%I, j.%I'
      END,
      c.column_name, c.column_name, c.column_name
    ), ', '
  )
  INTO from_column_aliases
  FROM information_schema.columns c
  WHERE c.table_schema = schema_name
  AND c.table_name = table_name_var
  AND c.column_name NOT IN ('id', 'journal_id', 'journal_timestamp', 'journal_operation', '_updated_at', 'updated_at', 'version', '_version', 'hash_code', '_hash_code');

  -- Generate the count statements for differences dynamically, creating multiple jsonb_build_object calls concatenated with ||
  SELECT string_agg(
    format(
      'jsonb_build_object(''%I'', NULLIF(count(*) filter (where from_journal_id is not null and %I IS DISTINCT FROM from_%I), 0))',
      c.column_name, c.column_name, c.column_name
    ), ' || '
  )
  INTO jsonb_concat
  FROM information_schema.columns c
  WHERE c.table_schema = schema_name
  AND c.table_name = table_name_var
  AND c.column_name NOT IN ('id', 'journal_id', 'journal_timestamp', 'journal_operation', '_updated_at', 'updated_at', 'version', '_version', 'hash_code', '_hash_code');

  -- Construct the dynamic SQL query
  query := format('
    WITH ids AS (
      SELECT DISTINCT id
      FROM %I.%I
    ), journals AS (
      SELECT
        id, journal_id, journal_timestamp, journal_operation,
        %s,
        lag(journal_id) OVER (PARTITION BY id ORDER BY journal_id ASC) AS from_journal_id
      FROM %I.%I
      WHERE id IN (SELECT id FROM ids)
    ), hydrated AS (
      SELECT
        j.id,
        o.journal_id as from_journal_id,
        j.journal_id,
        o.journal_timestamp as from_journal_timestamp,
        j.journal_timestamp,
        o.journal_operation as from_journal_operation,
        j.journal_operation,
        %s
      FROM journals j
      LEFT JOIN journals o ON j.from_journal_id = o.journal_id
    )
    SELECT
      count(distinct id) as distinct_ids,
      count(distinct id) filter (where from_journal_id is not null) as distinct_ids_with_history,
      count(journal_id) as distinct_journal_ids,
      count(journal_id) filter (where from_journal_id is not null) as distinct_journal_ids_with_previous,
      jsonb_pretty(jsonb_strip_nulls(%s)) as diff_counts
    FROM hydrated
  ',
  schema_name, table_name_var,
  column_names, schema_name, table_name_var,
  from_column_aliases,
  jsonb_concat
  );

  -- Execute the dynamic SQL query
  RETURN QUERY EXECUTE query;
END;
$$ LANGUAGE plpgsql;

