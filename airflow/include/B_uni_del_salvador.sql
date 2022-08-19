SELECT 
       svm.universidad AS university, 
       svm.carrera AS career,
       svm.fecha_de_inscripcion AS inscription_date,
       svm.nombre AS last_name,
       svm.sexo AS gender,
       svm.fecha_nacimiento as birth_day,
       NULL AS age,
       NULL AS postal_code,
       svm.localidad AS location,
       svm.email AS email
FROM salvador_villa_maria AS svm
WHERE TO_DATE(svm.fecha_de_inscripcion, 'DD-Mon-YY') >= '2020/09/01' 
AND TO_DATE(svm.fecha_de_inscripcion, 'DD-Mon-YY') <= '2021/02/01'
AND svm.universidad like 'UNIVERSIDAD_DEL_SALVADOR';
