SELECT 
	universities       AS university,
	trabajo            AS career,
	names              AS last_name,
    sexo               AS gender,
    birth_dates        AS birthdate,
    null               AS age,
    null               AS postal_code,
    locations          AS location,
    emails             AS email,
    to_date(lsc.inscription_dates, 'DD-MM-YYYY') as inscription_date
FROM lat_sociales_cine as lsc
WHERE lsc.universities = '-FACULTAD-LATINOAMERICANA-DE-CIENCIAS-SOCIALES' -- filter by university
AND to_date(lsc.inscription_dates, 'DD-MM-YYYY') BETWEEN '1-09-2020' AND '1-02-2021' -- filer by date

