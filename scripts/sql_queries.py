# Drop Tables
airport_codes_drop          = "DROP TABLE IF EXISTS airport_codes;"
code_country_drop           = "DROP TABLE IF EXISTS code_country;"
ports_drop                  = "DROP TABLE IF EXISTS ports;"
port_mode_drop              = "DROP TABLE IF EXISTS port_mode;"
states_drop                 = "DROP TABLE IF EXISTS states;"
i94_dates_drop              = "DROP TABLE IF EXISTS i94_dates;"
visa_category_drop          = "DROP TABLE IF EXISTS visa_category;"
world_temp_drop             = "DROP TABLE IF EXISTS world_temp;"
us_cities_demographics_drop = "DROP TABLE IF EXISTS us_cities_demographics;"
i94_immigration_drop        = "DROP TABLE IF EXISTS i94_immigration;"

# Create Tables
airport_codes_create          = ("""
CREATE TABLE airport_codes (
    ident VARCHAR NOT NULL,
    name VARCHAR,
    iso_country VARCHAR,
    iso_region VARCHAR,
    municipality VARCHAR,
    gps_code VARCHAR,
    iata_code VARCHAR,
    local_code VARCHAR
);
""")
code_country_create           = ("""
CREATE TABLE code_country (
    code INTEGER NOT NULL,
    country VARCHAR NOT NULL
);
""")
ports_create                  = ("""
CREATE TABLE ports (
    code VARCHAR NOT NULL,
    port VARCHAR NOT NULL,
    state_code VARCHAR NOT NULL
);
""")
port_mode_create              = ("""
CREATE TABLE port_mode (
    code INTEGER NOT NULL,
    mode VARCHAR NOT NULL
);
""")
states_create                 = ("""
CREATE TABLE states (
    state_code VARCHAR NOT NULL,
    state_name VARCHAR NOT NULL
);
""")
i94_dates_create              = ("""
CREATE TABLE i94_dates (
    full_date DATE NOT NULL,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    week INTEGER NOT NULL,
    weekday VARCHAR NOT NULL
);
""")
visa_category_create          = ("""
CREATE TABLE visa_category (
    code INTEGER NOT NULL,
    visa_category VARCHAR NOT NULL
);
""")
world_temp_create             = ("""
CREATE TABLE world_temp (
    City VARCHAR NOT NULL,
    Country VARCHAR NOT NULL,
    AvgTemp DOUBLE PRECISION,
    Country_Code INTEGER
);
""")
us_cities_demographics_create = ("""
CREATE TABLE us_cities_demographics (
    city VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    median_age DOUBLE PRECISION,
    male_population INTEGER,
    female_population INTEGER,
    total_population INTEGER,
    foreign_born INTEGER,
    state_code VARCHAR
);
""")
i94_immigration_create        = ("""
CREATE TABLE i94_immigration (
    cicid BIGINT NOT NULL,
    i94yr INTEGER NOT NULL,
    i94mon INTEGER NOT NULL,
    i94cit INTEGER,
    i94res INTEGER,
    i94port VARCHAR,
    arrdate DATE,
    i94mode INTEGER,
    i94addr VARCHAR,
    depdate DATE,
    i94bir INTEGER,
    i94visa INTEGER,
    biryear INTEGER,
    dtaddto VARCHAR,
    gender VARCHAR,
    airline VARCHAR,
    admnum BIGINT,
    fltno VARCHAR,
    visatype VARCHAR,
    matflag BOOLEAN
);
""")

# Copy Queries
airport_codes_copy           = ("""
    COPY airport_codes
    FROM 's3://capstone-staging-area/airport_codes/'
    credentials 'aws_iam_role={}'
    FORMAT AS PARQUET;
""")
code_country_copy            = ("""
    COPY code_country
    FROM 's3://capstone-staging-area/lookups/code_country.csv'
    credentials 'aws_iam_role={}'
    delimiter ',' CSV QUOTE '"' IGNOREHEADER 1 compupdate off region 'us-west-2';
""")
ports_copy                   = ("""
    COPY ports
    FROM 's3://capstone-staging-area/lookups/ports.csv'
    credentials 'aws_iam_role={}'
    delimiter ',' CSV QUOTE '"' IGNOREHEADER 1 compupdate off region 'us-west-2';
""")
port_mode_copy               = ("""
    COPY port_mode
    FROM 's3://capstone-staging-area/lookups/port_mode.csv'
    credentials 'aws_iam_role={}'
    delimiter ',' CSV QUOTE '"' IGNOREHEADER 1 compupdate off region 'us-west-2';
""")
states_copy                  = ("""
    COPY states
    FROM 's3://capstone-staging-area/lookups/states.csv'
    credentials 'aws_iam_role={}'
    delimiter ',' CSV QUOTE '"' IGNOREHEADER 1 compupdate off region 'us-west-2';
""")
visa_category_copy           = ("""
    COPY visa_category
    FROM 's3://capstone-staging-area/lookups/visa_category.csv'
    credentials 'aws_iam_role={}'
    delimiter ',' CSV QUOTE '"' IGNOREHEADER 1 compupdate off region 'us-west-2';
""")
world_temp_copy              = ("""
    COPY world_temp
    FROM 's3://capstone-staging-area/world_temp/'
    credentials 'aws_iam_role={}'
    FORMAT AS PARQUET;
""")
us_cities_demographics_copy  = ("""
    COPY us_cities_demographics
    FROM 's3://capstone-staging-area/us_cities_demographics/'
    credentials 'aws_iam_role={}'
    FORMAT AS PARQUET;
""")
i94_immigration_copy         = ("""
    COPY i94_immigration
    FROM 's3://capstone-staging-area/i94_immigration/i94'
    credentials 'aws_iam_role={}'
    FORMAT AS PARQUET;
""")

# Insert Queries
i94_dates_insert             = ("""
INSERT INTO i94_dates
SELECT  dt, 
        EXTRACT(DAY FROM dt) as day, 
        EXTRACT(MONTH FROM dt) as month, 
        EXTRACT(YEAR FROM dt) as year, 
        EXTRACT(WEEK FROM  dt) as week, 
        to_char(dt,'Day') as weekday
FROM
(
    SELECT DISTINCT dt
    FROM
    (
            SELECT DISTINCT arrdate as dt
            FROM i94_immigration
            WHERE arrdate IS NOT NULL
        UNION
            SELECT DISTINCT depdate as dt
            FROM i94_immigration
            WHERE depdate IS NOT NULL
    ) 
) as t4
""")


# Query Lists
drop_table_queries = [airport_codes_drop,code_country_drop,ports_drop,port_mode_drop,states_drop,i94_dates_drop,visa_category_drop,world_temp_drop,us_cities_demographics_drop,i94_immigration_drop]
create_table_queries = [airport_codes_create,code_country_create,ports_create,port_mode_create,states_create,i94_dates_create,visa_category_create,world_temp_create,us_cities_demographics_create,i94_immigration_create]