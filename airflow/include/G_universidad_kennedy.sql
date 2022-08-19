SELECT 
    universidades      AS university,
    trabajo            AS career,
    nombres            AS last_name,
    sexo               AS gender,
    fechas_nacimiento  AS birthdate,
    null			   AS age,
    codigos_postales   AS postal_code,
    null      		   AS location,
    emails             AS email,
    to_date(knd.fechas_de_inscripcion, 'YY-MON-DD') as inscription_date
FROM uba_kenedy as knd
WHERE knd.universidades = 'universidad-j.-f.-kennedy' -- filter by university
	AND to_date(knd.fechas_de_inscripcion, 'YY-MON-DD') BETWEEN '2020-09-01' AND '2021-02-01' -- filer by date

