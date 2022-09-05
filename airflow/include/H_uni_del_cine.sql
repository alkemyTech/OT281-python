
/*
Ticket:
https://alkemy-labs.atlassian.net/browse/OT281-20

COMO: Analista de datos
QUIERO: Escribir el código de dos consultas SQL, una para cada universidad.
PARA: Obtener los datos de las pesonas anotadas en entre las fechas 01-09-2020 al 01-02-2021 para las siguientes facultades:

university
career
inscription_date
first_name (lo sacamos por pedido del fer)
last_name
gender
birthdate
age
postal_code
location
email

*/


SELECT  
    universities as university,
	careers as career,
	to_date(T0.inscription_dates, 'DD-MM-YYY') as inscription_date,
	'null' as first_name,
	names as last_name, -- Se unifica first name a last name por directivas de fer
	sexo as gender,
	birth_dates as birth_date,
	'null' AS age, --Se le asigna null ya que no existe esta columna en la tabla(fer)
	'null' as postal_code, --Se le asigna null ya que no existe esta columna en la tabla(fer)
	locations as location,
	emails as email
FROM lat_sociales_cine as T0
where universities = 'UNIVERSIDAD-DEL-CINE'
AND	to_date(T0.inscription_dates, 'DD-MM-YYY') between '01-09-2020' and '01-02-2021'