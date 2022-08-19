SELECT 
university,
career,
inscription_date ,
nombre,
sexo,
birth_date ,
codigo_postal,
"location",
email    
from public.jujuy_utn
inner join localidad2
on localidad2.localidad = upper(jujuy_utn."location")
where university = 'universidad nacional de jujuy' and to_date(inscription_date,'YYYY/MM/DD') between '2020-09-01' and '2021-02-01' ;


-- The query load information from jujuy_utn table and join it with localidad2 table using the location field
-- The ON condition to join the tables has to be affected by the UPPER function because location fields in jujuy_utn were in lower case and localidad in localidad2 were in upper case
-- The where condition uses the inscription_date column but it has to been transformed in date format first


