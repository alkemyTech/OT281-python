select
universidad as university,
carrerra as career,
fechaiscripccion as inscription_date,
nombrre as last_name,
sexo as gender,
null as age,
nacimiento as birth_date,
codgoposstal as postal_code,
direccion as location,
eemail as email
from moron_nacional_pampa mnp
where universidad = 'Universidad de morón' 
and to_date (fechaiscripccion, 'DD/MM/YY')  between '01/09/2020' and '01/02/2021';