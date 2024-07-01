

select * from de_shop_customers_20230619
select * from de_shop_customers_20230901
select * from de_shop_customers_20240614
select * from de_shop_customers_20240701


CREATE TABLE unified_customers (
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    company_name VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(255),
    province VARCHAR(255),
    county VARCHAR(255),
    state VARCHAR(255),
    postal_code VARCHAR(20),
    phone1 VARCHAR(20),
    phone2 VARCHAR(20),
    email VARCHAR(255),
    web VARCHAR(255),
    source_file VARCHAR(50) -- To keep track of the source
);

-- Inserting from first table
INSERT INTO unified_customers (first_name, last_name, company_name, address, city, province, postal_code, phone1, phone2, email, web, source_file)
SELECT first_name, last_name, company_name, address, city, NULL as province, postal AS postal_code, phone1, phone2, email, web, '20230619' AS source_file
FROM de_shop_customers_20230619;




/*
Inserting from second table (no county column)
*/
INSERT INTO unified_customers (first_name, last_name, company_name, address, city, province, postal_code, phone1, phone2, email, web, source_file)
SELECT first_name, last_name, company_name, address, city, province, postal AS postal_code, phone1, phone2, email, web, '20230901' AS source_file
FROM de_shop_customers_20230901;





-- Inserting from third table (with state instead of county)
INSERT INTO unified_customers (first_name, last_name, company_name, address, city, province, postal_code, phone1, phone2, email, web, source_file)
SELECT first_name, last_name, company_name, address, city, NULL AS province, zip AS postal_code, phone1, phone2, email, web, '20240614' AS source_file
FROM de_shop_customers_20240614;






-- Inserting from fourth table (with state instead of county)
INSERT INTO unified_customers (first_name, last_name, company_name, address, city, province, county, state, postal_code, phone1, phone2, email, web, source_file)
SELECT first_name, last_name, company_name, address, city, NULL AS province, NULL AS county, state, post AS postal_code, phone1, phone2, email, web, '20240701' AS source_file
FROM de_shop_customers_20240701;



select * from        unified_customers

-- Deduplicate the unified_customers table

-- Create a new table to store unique records

CREATE TABLE unique_unified_customers AS
SELECT 
    first_name, 
    last_name, 
    company_name, 
    address, 
    city, 
    province,
    county, 
    state, 
    postal_code, 
    phone1, 
    phone2, 
    email, 
    web, 
    source_file
FROM 
    (SELECT 
        first_name, 
        last_name, 
        company_name, 
        address, 
        city, 
        province,
        county, 
        state, 
        postal_code, 
        phone1, 
        phone2, 
        email, 
        web, 
        source_file,
        ROW_NUMBER() OVER (PARTITION BY 
            first_name, 
            last_name, 
            company_name, 
            address, 
            city, 
            COALESCE(county, ''), 
            COALESCE(state, ''), 
            COALESCE(postal_code, ''), 
            phone1, 
            phone2, 
            email, 
            web, 
            source_file 
            ORDER BY first_name) AS row_num
     FROM unified_customers) AS temp
WHERE row_num = 1;

-- Drop the old table
DROP TABLE unified_customers;

-- Rename the new table to the original name
ALTER TABLE unique_unified_customers RENAME TO unified_customers;



-- Ensure the event scheduler is enabled
SET GLOBAL event_scheduler = ON;



DROP EVENT IF EXISTS ingest_customers;

CREATE EVENT ingest_customers
ON SCHEDULE EVERY 1 DAY
DO
CALL insert_customers();








