
select
names,
sexo,
correos_electronicos,
birth_dates,
universidad,
fecha_de_inscripcion,
careers,
palermo_tres_de_febrero.codigo_postal,
localidad
from palermo_tres_de_febrero
inner join
localidad2
on localidad2.codigo_postal = cast(palermo_tres_de_febrero.codigo_postal as int)
where universidad='_universidad_de_palermo' and to_date(fecha_de_inscripcion ,'DD/Mon/YY') between '2020-09-01' and '2021-02-01';



-- The query load information from palermo_tres_de_febrero table and the localidad2 table using the codigo_postal column
-- The ON condition to join the tables has to be affected by the CAST function because location fields in palermo_tres_de_febrero were in lower case and localidad in localidad2 were in upper case

-- The where condition uses the inscription_date column but it has to been transformed in date format first

