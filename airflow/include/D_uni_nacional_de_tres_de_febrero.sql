-- This query retrieves raw data (without processing) for all students from
-- 'Universidad Nacional de Tres de Febrero' inscripted between September 1, 2020 and February 1, 2021.

SELECT universidad AS university,
    careers AS career,
    fecha_de_inscripcion AS inscription_date,
    NULL AS first_name,
    names AS last_name, 
    sexo AS gender,
    birth_dates AS birth_date,
    NULL AS age,
    codigo_postal AS postal_code,
    NULL AS location,
    correos_electronicos AS email
FROM palermo_tres_de_febrero
WHERE LOWER(universidad) = 'universidad_nacional_de_tres_de_febrero'
    AND CAST (fecha_de_inscripcion AS DATE) >= '2020/09/01'
    AND CAST (fecha_de_inscripcion AS DATE) <= '2021/02/01';

-- Note that we use "LOWER(university)" to avoid caps issues while comparing strings.
-- In the same way we cast "inscription_date" as DATE (only in the where clause),
-- in order to avoid errors comparing dates with different formats.
-- Not found values are stored as NULL.