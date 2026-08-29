package migrations

import "github.com/pocketbase/dbx"

func createSQLiteEquivalentFunctions(db dbx.Builder) error {
	//PostgreSQL:
	// 1. Check existance
	sql := `SELECT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'uuid_generate_v7');`
	var exists bool
	if err := db.NewQuery(sql).Row(&exists); err != nil {
		return err
	} else if exists {
		// The function already exists, no need to create it again
		return nil
	}

	// Postgres:
	// 2. Create function
	funcDef := `
	-- Serialize concurrent bootstraps against the same database.
	-- CREATE EXTENSION/COLLATION/FUNCTION can deadlock on catalog locks
	-- when two app instances run this migration simultaneously on a fresh DB.
	SELECT pg_advisory_xact_lock(723390690);

	-- Enable built-in pgcrypto extension to use gen_random_bytes function
	CREATE EXTENSION IF NOT EXISTS "pgcrypto";

	-- Adding "nocase" collation to be compatible with SQLite's built-in "nocase" collation
	CREATE COLLATION IF NOT EXISTS "nocase" (
		provider = icu,          -- Specify ICU as the provider
		locale = 'und-u-ks-level2', -- Undetermined locale, Unicode extension (-u-), collation strength (ks) level 2 (level2)
		deterministic = false    -- Case-insensitive collations are typically non-deterministic
	);

	-- Alias [hex] to encode(..., 'hex')
	CREATE OR REPLACE FUNCTION hex(data bytea)
	RETURNS text
	LANGUAGE SQL
	IMMUTABLE
	AS $$
	SELECT encode(data, 'hex')
	$$;

	-- Alias [randomblob] to gen_random_bytes(...)
	CREATE OR REPLACE FUNCTION randomblob(length integer)
	RETURNS bytea
	LANGUAGE SQL
	IMMUTABLE
	AS $$
	SELECT gen_random_bytes(length)
	$$;

	-- Create the uuid_generate_v7 function
	create or replace function uuid_generate_v7()
		returns uuid
		as $$
		begin
		-- use random v4 uuid as starting point (which has the same variant we need)
		-- then overlay timestamp
		-- then set version 7 by flipping the 2 and 1 bit in the version 4 string
		return encode(
			set_bit(
			set_bit(
				overlay(uuid_send(gen_random_uuid())
						placing substring(int8send(floor(extract(epoch from clock_timestamp()) * 1000)::bigint) from 3)
						from 1 for 6
				),
				52, 1
			),
			53, 1
			),
			'hex')::uuid;
		end
		$$
		language plpgsql
		volatile;
	
	-- Create json_valid function
	CREATE OR REPLACE FUNCTION json_valid(text) RETURNS boolean AS $$
	BEGIN
		PERFORM $1::jsonb;
		RETURN TRUE;
	EXCEPTION WHEN others THEN
		RETURN FALSE;
	END;
	$$ LANGUAGE plpgsql IMMUTABLE;

	-- Create a json_query_or_null function that handles any types.
	CREATE OR REPLACE FUNCTION json_query_or_null(p_input jsonb, p_query text) RETURNS jsonb AS $$
		SELECT JSON_QUERY(p_input, p_query)
	$$ LANGUAGE sql IMMUTABLE;

	-- Create a json_query_or_null function that handles any types.
	CREATE OR REPLACE FUNCTION json_query_or_null(p_input anyelement, p_query text) RETURNS jsonb AS $$
	BEGIN
		RETURN JSON_QUERY(p_input::text::jsonb, p_query);
	EXCEPTION WHEN others THEN
		RETURN NULL;
	END;
	$$ LANGUAGE plpgsql STABLE;
-- Create total aggregate function compatible with SQLite's total()
-- (https://sqlite.org/lang_aggfunc.html#total).
-- Unlike sum(), total() returns numeric sum for any input and never returns NULL.
CREATE OR REPLACE FUNCTION total_sfunc(numeric, anyelement) RETURNS numeric AS $$
BEGIN
	RETURN coalesce($1, 0) + coalesce(nullif($2::text, '')::numeric, 0);
EXCEPTION WHEN others THEN
	RETURN coalesce($1, 0);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE AGGREGATE total(anyelement) (
	SFUNC = total_sfunc,
	STYPE = numeric,
	INITCOND = '0.0'
);
-- Create strftime functions compatible with SQLite's strftime
-- (https://sqlite.org/lang_datefunc.html).
-- Note: impl is STABLE because the single-argument overload resolves
-- the "current time" (like SQLite) which must not be IMMUTABLE.
CREATE OR REPLACE FUNCTION strftime_impl(p_format text, p_ts timestamptz, p_modifiers text[])
RETURNS text
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
	v_ts timestamp with time zone := p_ts;
	v_local boolean := false;
	v_render timestamp without time zone;
	i int;
	m text;
	n numeric;
	unit text;
	delta int;
	result text := '';
	pos int := 1;
	ch text;
	spec text;
	y int;
	mo int;
	d int;
	h int;
	mi int;
	sec numeric;
	doy int;
	dow int;
BEGIN
	IF v_ts IS NULL THEN
		RETURN NULL;
	END IF;

	-- apply the modifiers in order
	FOR i IN 1..coalesce(array_length(p_modifiers, 1), 0) LOOP
		m := lower(btrim(p_modifiers[i]));
		IF m = 'localtime' THEN
			v_local := true;
		ELSIF m = 'utc' THEN
			v_local := false;
		ELSIF m IN ('start of month', 'start of year', 'start of day') THEN
			v_ts := date_trunc(substring(m from 10), v_ts);
		ELSIF m ~ '^[+-]?[0-9]+(\.[0-9]+)?\s+(years?|months?|days?|hours?|minutes?|seconds?|milliseconds?)$' THEN
			n := substring(m from '(^[+-]?[0-9]+(\.[0-9]+)?)')::numeric;
			unit := substring(m from '(years?|months?|days?|hours?|minutes?|seconds?|milliseconds?)$');
			unit := rtrim(unit, 's');
			v_ts := v_ts + n * ('1 ' || unit)::interval;
		ELSIF m ~ '^weekday [0-6]$' THEN
			-- advance to the next date with the given weekday (0=sunday)
			delta := substring(m from '([0-6]$)')::int;
			v_ts := v_ts + (((delta - extract(dow from v_ts)::int) % 7) + 7) % 7 * interval '1 day';
		ELSE
			RAISE EXCEPTION 'unknown strftime modifier: %', m;
		END IF;
	END LOOP;

	v_render := v_ts AT TIME ZONE (CASE WHEN v_local THEN current_setting('TimeZone') ELSE 'UTC' END);

	y := extract(year from v_render)::int;
	mo := extract(month from v_render)::int;
	d := extract(day from v_render)::int;
	h := extract(hour from v_render)::int;
	mi := extract(minute from v_render)::int;
	sec := extract(second from v_render);
	doy := extract(doy from v_render)::int;
	dow := extract(dow from v_render)::int;

	WHILE pos <= length(p_format) LOOP
		ch := substr(p_format, pos, 1);
		IF ch = '%' AND pos < length(p_format) THEN
			spec := substr(p_format, pos + 1, 1);
			CASE spec
				WHEN 'Y' THEN result := result || to_char(y, 'FM0000');
				WHEN 'm' THEN result := result || to_char(mo, 'FM00');
				WHEN 'd' THEN result := result || to_char(d, 'FM00');
				WHEN 'H' THEN result := result || to_char(h, 'FM00');
				WHEN 'k' THEN result := result || h::text;
				WHEN 'I' THEN result := result || to_char(((h + 11) % 12) + 1, 'FM00');
				WHEN 'l' THEN result := result || (((h + 11) % 12) + 1)::text;
				WHEN 'M' THEN result := result || to_char(mi, 'FM00');
				WHEN 'S' THEN result := result || to_char(floor(sec)::int, 'FM00');
				WHEN 'f' THEN result := result || to_char(floor(sec)::int, 'FM00') || '.' || to_char(floor((sec - floor(sec)) * 1000)::int, 'FM000');
				WHEN 'j' THEN result := result || to_char(doy, 'FM000');
				WHEN 'J' THEN result := result || round(extract(epoch from v_ts) / 86400.0 + 2440587.5, 4)::text;
				WHEN 'p' THEN result := result || CASE WHEN h < 12 THEN 'AM' ELSE 'PM' END;
				WHEN 'P' THEN result := result || CASE WHEN h < 12 THEN 'am' ELSE 'pm' END;
				WHEN 's' THEN result := result || floor(extract(epoch from v_ts))::bigint::text;
				WHEN 'w' THEN result := result || dow::text;
				WHEN 'W' THEN result := result || to_char(((doy - 1) + 7 - ((dow + 6) % 7)) / 7, 'FM00');
				WHEN '%' THEN result := result || '%';
				ELSE result := result || ch || spec;
			END CASE;
			pos := pos + 2;
		ELSE
			result := result || ch;
			pos := pos + 1;
		END IF;
	END LOOP;

	RETURN result;
END;
$$;

-- strftime(format) resolves the time-value to the current time (as in SQLite)
CREATE OR REPLACE FUNCTION strftime(p_format text)
RETURNS text
LANGUAGE sql
STABLE
AS $$
	SELECT strftime_impl(p_format, now(), ARRAY[]::text[])
$$;

-- strftime(format, text-timevalue, modifiers...)
CREATE OR REPLACE FUNCTION strftime(p_format text, p_timevalue text, VARIADIC p_modifiers text[] DEFAULT ARRAY[]::text[])
RETURNS text
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
	v_ts timestamptz;
	v_raw text := btrim(p_timevalue);
	v_mods text[] := p_modifiers;
BEGIN
	IF v_raw IS NULL OR v_raw = '' THEN
		RETURN NULL;
	END IF;

	IF lower(v_raw) = 'now' THEN
		v_ts := now();
	ELSIF v_raw ~ '^-?[0-9]+(\.[0-9]+)?$' THEN
		IF coalesce(array_length(v_mods, 1), 0) > 0 AND lower(btrim(v_mods[1])) = 'unixepoch' THEN
			-- unix epoch seconds (consume the modifier)
			v_ts := to_timestamp(v_raw::numeric);
			v_mods := coalesce(v_mods[2:], ARRAY[]::text[]);
		ELSE
			-- julian day number (SQLite time-value format 12)
			v_ts := to_timestamp((v_raw::numeric - 2440587.5) * 86400);
		END IF;
	ELSIF v_raw ~ '^[0-9]{1,2}:[0-9]{2}(:[0-9]{2}(\.[0-9]+)?)?$' THEN
		-- time-only value is resolved against the 2000-01-01 base date (as in SQLite)
		v_ts := ('2000-01-01 ' || v_raw || '+00')::timestamptz;
	ELSE
		-- date/datetime string; treat as UTC when no explicit zone offset is present
		IF v_raw !~ '([zZ]|[+-][0-9]{2}:?[0-9]{2})$' THEN
			v_raw := v_raw || '+00';
		END IF;
		BEGIN
			v_ts := v_raw::timestamptz;
		EXCEPTION WHEN others THEN
			RETURN NULL;
		END;
	END IF;

	RETURN strftime_impl(p_format, v_ts, v_mods);
END;
$$;

-- strftime(format, numeric-timevalue, modifiers...)
-- Note: bare numbers are interpreted as Julian day numbers (SQLite time-value
-- format 12), or as unix epoch seconds when the "unixepoch" modifier is used.
CREATE OR REPLACE FUNCTION strftime(p_format text, p_timevalue numeric, VARIADIC p_modifiers text[] DEFAULT ARRAY[]::text[])
RETURNS text
LANGUAGE sql
STABLE
AS $$
	SELECT strftime(p_format, p_timevalue::text, VARIADIC p_modifiers)
$$;

-- strftime(format, timestamp-timevalue, modifiers...)
CREATE OR REPLACE FUNCTION strftime(p_format text, p_timevalue timestamp, VARIADIC p_modifiers text[] DEFAULT ARRAY[]::text[])
RETURNS text
LANGUAGE sql
STABLE
AS $$
	SELECT strftime_impl(p_format, p_timevalue::timestamptz, p_modifiers)
$$;
	`
	_, err := db.NewQuery(funcDef).Execute()
	return err
}
