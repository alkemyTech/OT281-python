SELECT 
       svm.universidad AS university, 
       svm.carrera AS career,
       svm.fecha_de_inscripcion AS inscription_date,
       svm.nombre as first_name,
       svm.nombre AS last_name,
       svm.sexo AS gender,
       svm.fecha_nacimiento as age,
       l2.codigo_postal AS postal_code,
       svm.localidad AS location,
       svm.email AS email
FROM salvador_villa_maria AS svm
LEFT JOIN localidad2 AS l2
ON replace(svm.localidad, '_', ' ') = l2.localidad
WHERE TO_DATE(svm.fecha_de_inscripcion, 'DD-Mon-YY') >= '2020/09/01' 
AND TO_DATE(svm.fecha_de_inscripcion, 'DD-Mon-YY') <= '2021/02/01'
AND svm.universidad like 'UNIVERSIDAD_DEL_SALVADOR';
