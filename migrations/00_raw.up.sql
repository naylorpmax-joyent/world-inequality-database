USE raw;

CREATE TABLE IF NOT EXISTS country (
    alpha2 CHAR(6),
    titlename VARCHAR(100),
    shortname VARCHAR(100),
    region VARCHAR(100),
    region2 VARCHAR(100),

    PRIMARY KEY pk_country (alpha2)
);

CREATE TABLE IF NOT EXISTS metadata (
    country CHAR(6),
    variable VARCHAR(20),
    age INT,
    pop CHAR(1),
    countryname VARCHAR(100),
    shortname TEXT,
    simpledes TEXT,
    technicaldes TEXT,
    shorttype VARCHAR(100),
    longtype TEXT,
    shortpop VARCHAR(100),
    longpop TEXT,
    shortage VARCHAR(100),
    longage TEXT,
    unit TEXT,
    source TEXT,
    method TEXT,
    extrapolation TEXT,
    data_points TEXT,

 	PRIMARY KEY pk_metadata (country, variable),
  	FOREIGN KEY fk_raw_metadata_alpha2 (country) REFERENCES country (alpha2)
);

CREATE TABLE IF NOT EXISTS data (
    country CHAR(6),
    variable VARCHAR(100),
    percentile VARCHAR(100),
    year SMALLINT,
    value FLOAT(20),
    age INT,
    pop CHAR(1)
);
