SELECT 
       fc.universidad AS university, 
       fc.carrera AS career,
       fc.fecha_de_inscripcion AS inscription_date,
       fc.name AS last_name,
       fc.sexo AS gender,
       fc.fecha_nacimiento AS birth_day,
       NULL AS age,
       fc.codigo_postal AS postal_code,
       NULL AS location,
       fc.correo_electronico AS email
FROM flores_comahue AS fc
WHERE fecha_de_inscripcion >= '2020/09/01' 
AND fecha_de_inscripcion <= '2021/02/01'
AND universidad like 'UNIV. NACIONAL DEL COMAHUE';
