SELECT 
university,
career,
inscription_date ,
nombre as last_name,
sexo as gender,
birth_date ,
NULL as age,
NULL as postal_code,
"location",
email    
from public.jujuy_utn
where university = 'universidad nacional de jujuy' and to_date(inscription_date,'YYYY/MM/DD') between '2020-09-01' and '2021-02-01' ;


-- The query load information from jujuy_utn table 
-- The where condition uses the inscription_date column but it has to been transformed in date format first
