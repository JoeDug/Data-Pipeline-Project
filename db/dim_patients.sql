--I have made a change
CREATE OR REPLACE TABLE dim_patients AS
SELECT
    id AS patient_id,
    birthdate,
    deathdate,
    CAST(DATE_DIFF('day', birthdate, CURRENT_DATE) / 365.25 AS INTEGER) AS age,
    deathdate IS NOT NULL AS is_deceased,
    gender,
    race,
    ethnicity,
    marital,
    city,
    state,
    zip
FROM staging_patients;
