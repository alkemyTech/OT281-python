-- This query retrieves raw data (without processing) for all students from
-- 'Universidad Tecnológica Nacional' inscripted between September 1, 2020 and February 1, 2021.

SELECT university AS university,
    career AS career,
    inscription_date AS inscription_date,
    NULL AS first_name,
    nombre AS last_name, 
    sexo AS gender,
    birth_date AS birth_date,
    NULL AS age,
    NULL AS postal_code,
    location AS location,
    email AS email
FROM jujuy_utn
WHERE LOWER(university) = 'universidad tecnológica nacional'
    AND CAST(inscription_date AS DATE) >= '2020/09/01'
    AND CAST(inscription_date AS DATE) <= '2021/02/01';

-- Note that we use "LOWER(university)" to avoid caps issues while comparing strings.
-- In the same way we cast "inscription_date" as DATE (only in the where clause),
-- in order to avoid errors comparing dates with different formats.
-- Not found values are stored as NULL.