-- This query retrieves raw data (without processing) for all students from
-- 'Universidad Nacional de Tres de Febrero' inscripted between September 1, 2020 and February 1, 2021.

SELECT universidad AS university,
careers AS career,
fecha_de_inscripcion AS inscription_date,
names AS last_name, 
sexo AS gender,
birth_dates AS age,
l.codigo_postal AS postal_code, 
l.localidad AS location,
correos_electronicos AS email
FROM palermo_tres_de_febrero as ptdf 
JOIN localidad2 AS l
ON CAST (ptdf.codigo_postal AS INT) = l.codigo_postal
WHERE universidad = 'universidad_nacional_de_tres_de_febrero'
AND CAST (fecha_de_inscripcion AS DATE) >= '2020/09/01'
AND CAST (fecha_de_inscripcion AS DATE) <= '2021/02/01';

-- Note that the column 'age' is in fact the birth date, this is intentionally, since we want raw data
-- that will be processed later on further steps.
-- Same thing with 'last_name' actually storing the full name.