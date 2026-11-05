
-- Clean and standardize DOB NOW Job Application Filings
-- One row per job filing number

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'source_dob_job_applications_homework') }}
),

cleaned AS (
    SELECT
        -- Preserve all columns except those transformed below
        * EXCEPT (
            job_filing_number,
            filing_status,
            borough,
            house_no,
            street_name,
            block,
            lot,
            bin,
            commmunity_board,
            postcode,
            initial_cost,
            total_construction_floor_area,
            existing_stories,
            existing_height,
            existing_dwelling_units,
            proposed_no_of_stories,
            proposed_height,
            proposed_dwelling_units,
            filing_date,
            current_status_date,
            first_permit_date,
            approved_date,
            signoff_date,
            latitude,
            longitude,
            job_type,
            building_type
        ),

        -- Identifiers
        NULLIF(TRIM(job_filing_number), '') AS job_filing_number,

        -- Filing information
        UPPER(TRIM(filing_status)) AS filing_status,
        UPPER(TRIM(job_type)) AS job_type,
        UPPER(TRIM(building_type)) AS building_type,

        -- Standardize borough
        CASE
            WHEN UPPER(TRIM(borough)) IN
                ('MANHATTAN', 'NEW YORK', 'NEW YORK COUNTY', '1')
                THEN 'Manhattan'
            WHEN UPPER(TRIM(borough)) IN
                ('BRONX', 'THE BRONX', '2')
                THEN 'Bronx'
            WHEN UPPER(TRIM(borough)) IN
                ('BROOKLYN', 'KINGS COUNTY', '3')
                THEN 'Brooklyn'
            WHEN UPPER(TRIM(borough)) IN
                ('QUEENS', 'QUEENS COUNTY', '4')
                THEN 'Queens'
            WHEN UPPER(TRIM(borough)) IN
                ('STATEN ISLAND', 'RICHMOND COUNTY', '5')
                THEN 'Staten Island'
            ELSE 'Unknown'
        END AS borough,

        -- Property details
        NULLIF(TRIM(house_no), '') AS house_no,
        NULLIF(TRIM(street_name), '') AS street_name,
        NULLIF(TRIM(block), '') AS block,
        NULLIF(TRIM(lot), '') AS lot,
        NULLIF(TRIM(bin), '') AS bin,
        NULLIF(TRIM(commmunity_board), '') AS community_board,

        -- Property ZIP code
        CASE
            WHEN REGEXP_CONTAINS(
                TRIM(postcode),
                r'^\d{5}(-\d{4})?$'
            )
            THEN TRIM(postcode)
            ELSE NULL
        END AS postcode,

        -- Financial information
        SAFE_CAST(
            REGEXP_REPLACE(initial_cost, r'[$,\s]', '')
            AS NUMERIC
        ) AS initial_cost,

        -- Construction floor area
        SAFE_CAST(
            REGEXP_REPLACE(
                total_construction_floor_area, r'[,\s]', ''
            ) AS NUMERIC
        ) AS total_construction_floor_area,

        -- Existing building characteristics
        SAFE_CAST(
            REGEXP_REPLACE(existing_stories, r'[,\s]', '')
            AS NUMERIC
        ) AS existing_stories,

        SAFE_CAST(
            REGEXP_REPLACE(existing_height, r'[,\s]', '')
            AS NUMERIC
        ) AS existing_height,

        SAFE_CAST(
            REGEXP_REPLACE(existing_dwelling_units, r'[,\s]', '')
            AS INT64
        ) AS existing_dwelling_units,

        -- Proposed building characteristics
        SAFE_CAST(
            REGEXP_REPLACE(proposed_no_of_stories, r'[,\s]', '')
            AS NUMERIC
        ) AS proposed_no_of_stories,

        SAFE_CAST(
            REGEXP_REPLACE(proposed_height, r'[,\s]', '')
            AS NUMERIC
        ) AS proposed_height,

        SAFE_CAST(
            REGEXP_REPLACE(proposed_dwelling_units, r'[,\s]', '')
            AS INT64
        ) AS proposed_dwelling_units,

        -- Dates: already TIMESTAMP in BigQuery
        CAST(filing_date AS TIMESTAMP) AS filing_date,
        CAST(current_status_date AS TIMESTAMP) AS current_status_date,
        CAST(first_permit_date AS TIMESTAMP) AS first_permit_date,
        CAST(approved_date AS TIMESTAMP) AS approved_date,
        CAST(signoff_date AS TIMESTAMP) AS signoff_date,

        -- Geographic coordinates
        SAFE_CAST(latitude AS FLOAT64) AS latitude,
        SAFE_CAST(longitude AS FLOAT64) AS longitude,

        -- Metadata
        CURRENT_TIMESTAMP() AS _stg_loaded_at

    FROM source

    -- Filters
    WHERE job_filing_number IS NOT NULL
      AND TRIM(job_filing_number) != ''

    -- Deduplication: keep the latest record per filing
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TRIM(job_filing_number)
        ORDER BY
            current_status_date DESC NULLS LAST,
            filing_date DESC NULLS LAST
    ) = 1
)

SELECT * FROM cleaned
