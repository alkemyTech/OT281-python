/*

https://alkemy-labs.atlassian.net/browse/OT281-20

COMO: Analista de datos
QUIERO: Escribir el código de dos consultas SQL, una para cada universidad.
PARA: Obtener los datos de las pesonas anotadas en entre las fechas 01/9/2020 al 01/02/2021 para las siguientes facultades:

Universidad Del Cine

Universidad De Buenos Aires

Criterios de aceptación: 
Deben presentar la consulta en un archivo .sql. La consulta debe disponibilizar únicamente la 
información necesaria para que en un futuro sea procesada y genere los siguientes datos para las fechas indicadas.
Datos esperados:

university
career
inscription_date
first_name
last_name
gender
age
postal_code
location
email

Aclaración: Las tablas tienen dos universidades cada una. No hace falta interpretar datos que no parezcan lógicos como 
fechas de nacimiento y de inscripción fuera del rango de interés. Lo importante es traer 
toda la información de la base de datos en las fechas especificadas y cada tarea se debe ejecutar 5 veces antes de fallar.*/

SELECT 
    universidades as university,
	carreras as career,
	fechas_de_inscripcion as inscription_date,
	-- Se unifica first name a last name por directivas de fer
	nombres as last_name,
	sexo as gender,
	to_date(fechas_nacimiento, 'YY-Mon-DD') as birth_date,
	'null' AS age,
	codigos_postales as postal_code,
	direcciones as location,
	emails as email
FROM uba_kenedy as T0
where universidades = 'universidad-de-buenos-aires'
AND	to_date(fechas_de_inscripcion, 'YY-Mon-DD') BETWEEN '01-Sep-20' AND '01-Feb-21'
