CREATE TYPE continent AS ENUM (
    'Africa',
    'Asia',
    'Europe',
    'North America', 
    'Oceania',
    'South America'
);

CREATE TABLE country (
    id UUID PRIMARY KEY,
    country varchar,
    continent continent
);

CREATE TABLE person (
    id UUID PRIMARY KEY,
    full_name varchar,
    birth date,
    country_id UUID REFERENCES country (id)
);
