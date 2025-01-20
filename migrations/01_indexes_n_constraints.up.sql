CREATE INDEX IF NOT EXISTS idx_data_country_variable
ON raw.data (country, variable, year);

ALTER TABLE raw.data
ADD CONSTRAINT fk_data_country_variable
FOREIGN KEY IF NOT EXISTS (country, variable)
REFERENCES raw.metadata (country, variable);
