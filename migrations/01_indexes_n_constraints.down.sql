DROP INDEX IF EXISTS idx_data_country_variable;

ALTER TABLE raw.data
DROP CONSTRAINT IF EXISTS fk_data_country_variable;
