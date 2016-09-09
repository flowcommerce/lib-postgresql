create or replace function journal.get_data_type_string(
  p_column information_schema.columns
) returns varchar language plpgsql as $$
declare
  journal_data_type   text;
begin
  IF p_column.data_type = 'numeric' THEN
    IF p_column.numeric_precision IS NOT NULL AND p_column.numeric_scale IS NOT NULL THEN
      journal_data_type := p_column.data_type || '(' || p_column.numeric_precision::varchar || ',' || p_column.numeric_scale::varchar || ')';
    ELSE
      journal_data_type := p_column.data_type;
    END IF;
  ELSEIF p_column.data_type IN ('character', 'character varying', '"char"') THEN
    journal_data_type := 'text';
  ELSE
    journal_data_type := p_column.data_type;
  END IF;

  return journal_data_type;
end;
$$;

