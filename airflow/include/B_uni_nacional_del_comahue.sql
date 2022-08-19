SELECT 
       fc.universidad AS university, 
       fc.carrera AS career,
       fc.fecha_de_inscripcion AS inscription_date,
       SPLIT_PART(fc.name, ' ', 1) AS first_name,
       SPLIT_PART(fc.name, ' ', 2) AS last_name,
       fc.sexo AS gender,
       (CURRENT_DATE - TO_DATE(fc.fecha_nacimiento, 'YYYY/MM/DD'))/365 AS age,
       fc.codigo_postal AS postal_code,
       l2.localidad AS location,
       fc.correo_electronico AS email
FROM flores_comahue AS fc
LEFT JOIN localidad2 AS l2
ON CAST(fc.codigo_postal AS INTEGER) = l2.codigo_postal
WHERE fecha_de_inscripcion > '2020/09/01' 
AND fecha_de_inscripcion < '2021/02/01'
AND universidad like 'UNIV. NACIONAL DEL COMAHUE';
