-- The following script retrives the necessary information related to fulfill OT281-13 story
-- for Alkemy-DA-Python

-- It selects college enrolled students from Universidad Nacional de Villa Maria
-- from September 20' to February 21' 

select 
	universidad as university, 
	carrera as career,
	fecha_de_inscripcion as inscription_date,
	null as first_name,
	nombre as last_name,
	sexo as gender,
	fecha_nacimiento as birth_day, 
	null as age,
	null as postal_code,
	localidad as location,
	email 
from
	salvador_villa_maria
where
	universidad = 'UNIVERSIDAD_NACIONAL_DE_VILLA_MARÍA'
and
	to_date(fecha_de_inscripcion, 'YY-Mon-DD') >= '2020/09/01' 
and
	to_date(fecha_de_inscripcion, 'YY-Mon-DD') <= '2021/02/01';
