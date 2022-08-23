/* This query selects the certain data from the table "rio_cuarto_interamericana"*/
SELECT univiersities AS university,
    carrera AS career,
    inscription_dates AS inscription_date,
    names AS first_name,
    NULL AS last_name,
    sexo AS gender,
    NULL AS age,
    fechas_nacimiento AS birth_date,
    NULL AS postal_code,
    localidad AS location,
    email
FROM rio_cuarto_interamericana
WHERE TO_DATE(inscription_dates,'YY/MON/DD') BETWEEN '2020-09-01' AND '2021-02-01'
AND univiersities like '-universidad-abierta-interamericana';

