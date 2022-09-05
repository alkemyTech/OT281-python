
select
universidad as university,
careers,
fecha_de_inscripcion as inscription_date,
names as last_name,
sexo as gender,
null as age,
birth_dates as birth_date,
codigo_postal as postal_code,
null as location,
correos_electronicos as email

from palermo_tres_de_febrero
where universidad='_universidad_de_palermo' and to_date(fecha_de_inscripcion ,'DD/Mon/YY') between '2020-09-01' and '2021-02-01';



-- The query load information from palermo_tres_de_febrero table 
-- The where condition uses the inscription_date column but it has to been transformed in date format first

