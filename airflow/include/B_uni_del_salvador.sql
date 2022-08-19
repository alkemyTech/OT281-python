SELECT 
       svm.universidad AS university, 
       svm.carrera AS career,
       svm.fecha_de_inscripcion AS inscription_date,
       SPLIT_PART(svm.nombre, '_', 1) AS first_name,
       SPLIT_PART(svm.nombre, '_', 2) AS last_name,
       svm.sexo AS gender,
       (CURRENT_DATE - TO_DATE(CASE
                                   WHEN CAST(SPLIT_PART(svm.fecha_nacimiento, '-', 3) as INTEGER) >= 07
                                   THEN CONCAT(
                                       SUBSTRING(svm.fecha_nacimiento, 1, 6), '-19', 
                                       SPLIT_PART(svm.fecha_nacimiento, '-', 3))
                                   ELSE CONCAT(
                                       SUBSTRING(svm.fecha_nacimiento, 1, 6), '-20', 
                                       SPLIT_PART(svm.fecha_nacimiento, '-', 3))
      END, 'DD-Mon-YY'))/365 AS age,
       l2.codigo_postal AS postal_code,
       svm.localidad AS location,
       svm.email AS email
FROM salvador_villa_maria AS svm
LEFT JOIN localidad2 AS l2
ON replace(svm.localidad, '_', ' ') = l2.localidad
WHERE TO_DATE(svm.fecha_de_inscripcion, 'DD-Mon-YY') > '2020/09/01' 
AND TO_DATE(svm.fecha_de_inscripcion, 'DD-Mon-YY') < '2021/02/01'
AND svm.universidad like 'UNIVERSIDAD_DEL_SALVADOR';
