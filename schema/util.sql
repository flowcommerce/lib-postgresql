create schema util;
set search_path to util;

create or replace function delete_by_id(
  p_user_id text,
  p_table_name text,
  p_id text
) returns void language plpgsql as $$
begin
  execute 'set journal.deleted_by_user_id = ''' || p_user_id || '''';
  execute 'delete from ' || p_table_name || ' where id = ''' || p_id || '''';
end;
$$;

create or replace function non_empty_trimmed_string(p_value text) returns boolean immutable cost 1 language plpgsql as $$
  begin
    if p_value is null or trim(p_value) = '' or trim(p_value) != p_value then
      return false;
    else
      return true;
    end if;
  end
$$;

create or replace function null_or_lower_non_empty_trimmed_string(p_value text) returns boolean immutable cost 1 language plpgsql as $$
  begin
    if p_value is null then
      return true;
    else
      if lower(trim(p_value)) = p_value and p_value != '' then
        return true;
      else
        return false;
      end if;
    end if;
  end
$$;

create or replace function null_or_upper_non_empty_trimmed_string(p_value text) returns boolean immutable cost 1 language plpgsql as $$
  begin
    if p_value is null then
      return true;
    else
      if upper(trim(p_value)) = p_value and p_value != '' then
        return true;
      else
        return false;
      end if;
    end if;
  end
$$;

create or replace function null_or_non_empty_trimmed_string(p_value text) returns boolean immutable cost 1 language plpgsql as $$
  begin
    if p_value is null then
      return true;
    else
      if trim(p_value) = p_value and p_value != '' then
        return true;
      else
        return false;
      end if;
    end if;
  end
$$;

create or replace function lower_non_empty_trimmed_string(p_value text) returns boolean immutable cost 1 language plpgsql as $$
  begin
    if (util.non_empty_trimmed_string(p_value) and lower(p_value) = p_value) then
      return true;
    else
      return false;
    end if;
  end
$$;

create or replace function upper_non_empty_trimmed_string(p_value text) returns boolean immutable cost 1 language plpgsql as $$
  begin
    if (util.non_empty_trimmed_string(p_value) and upper(p_value) = p_value) then
      return true;
    else
      return false;
    end if;
  end
$$;

create or replace function null_or_currency_code(p_value text) returns boolean immutable cost 1 language plpgsql as $$
  begin
    if p_value is null or util.currency_code(p_value) then
      return true;
    else
      return false;
    end if;
  end
$$;

create or replace function currency_code(p_value text) returns boolean immutable cost 1 language plpgsql as $$
  begin
    if upper(trim(p_value)) = p_value and length(p_value) = 3 then
      return true;
    else
      return false;
    end if;
  end
$$;

create or replace function null_or_country_code(p_value text) returns boolean immutable cost 1 language plpgsql as $$
  begin
    if p_value is null or util.country_code(p_value) then
      return true;
    else
      return false;
    end if;
  end
$$;

create or replace function country_code(p_value text) returns boolean immutable cost 1 language plpgsql as $$
  begin
    if upper(trim(p_value)) = p_value and length(p_value) = 3 then
      return true;
    else
      return false;
    end if;
  end
$$;

create or replace function null_or_language_code(p_value text) returns boolean immutable cost 1 language plpgsql as $$
  begin
    if p_value is null or util.language_code(p_value) then
      return true;
    else
      return false;
    end if;
  end
$$;

create or replace function language_code(p_value text) returns boolean immutable cost 1 language plpgsql as $$
  begin
    if lower(trim(p_value)) = p_value and length(p_value) = 2 then
      return true;
    else
      return false;
    end if;
  end
$$;
