-- The following script retrives the necessary information related to fulfill OT281-13 story
-- for Alkemy-DA-Python

-- It selects college enrolled students from Universidad de Flores from September 20' to February 21' 

select
	universidad as university, 
	carrera as career,
	fecha_de_inscripcion as inscription_date,
	null as first_name,
	name as last_name,
	sexo as gender,
	fecha_nacimiento as birth_day, 
	null as age,
	codigo_postal as postal_code,
	null as location,
	correo_electronico as email 
from
	flores_comahue
where
	universidad = 'UNIVERSIDAD DE FLORES'
and 
	fecha_de_inscripcion between '2020-09-01' and '2021-02-01';
