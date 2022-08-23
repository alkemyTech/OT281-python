select
univiersities as university,
carrera as career,
inscription_dates as inscription_date,
names as last_name,
sexo as gender,
null as age,
fechas_nacimiento as birth_date,
null as postal_code,
direcciones as location,
email  
from rio_cuarto_interamericana rci 
where univiersities  = 'Universidad-nacional-de-río-cuarto' 
and to_date (inscription_dates, 'DD/Mon/YY')  between '01/09/2020' and '01/02/2021';