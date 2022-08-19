-- This query retrieves raw data (without processing) for all students from
-- 'Universidad Tecnológica Nacional' inscripted between September 1, 2020 and February 1, 2021.

SELECT university AS university,
career AS career,
inscription_date AS inscription_date,
nombre AS last_name, 
sexo AS gender,
birth_date AS age,
l.codigo_postal AS postal_code, 
l.localidad AS location,
email AS email
FROM jujuy_utn AS jutn
JOIN localidad2 AS l
ON UPPER(jutn.location) = UPPER(l.localidad)
WHERE LOWER(university) = 'universidad tecnológica nacional'
AND inscription_date >= '2020/09/01'
AND inscription_date <= '2021/02/01';

-- Note that the column 'age' is in fact the birth date, this is intentionally, since we want raw data
-- that will be processed later on further steps.
-- Same thing with 'last_name' actually storing the full name.