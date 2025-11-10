-- 000_create_db_if_not_exists.sql
CREATE EXTENSION IF NOT EXISTS dblink;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'chronosched'
  ) THEN
    PERFORM dblink_exec('dbname=' || current_database(),
      'CREATE DATABASE chronosched WITH OWNER = postgres ENCODING = ''UTF8''');
  END IF;
END $$;
