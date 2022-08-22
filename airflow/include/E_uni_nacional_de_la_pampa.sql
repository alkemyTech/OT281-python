/*This query selects the certain data from the table "moron_nacional_pampa" from */
SELECT universidad AS university,
    carrerra AS career,
    fechaiscripccion AS inscription_date,
    nombrre AS first_name,
    NULL AS last_name,
    sexo AS gender,
    NULL AS age,
    nacimiento AS birth_date,
    codgoposstal AS postal_code,
    NULL AS location,
    eemail AS email
FROM moron_nacional_pampa
WHERE TO_DATE(fechaiscripccion,'DD/MM/YY') BETWEEN '2020-09-01' AND '2021-02-01'
AND universidad like 'Universidad nacional de la pampa';