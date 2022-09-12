from re import template
from jinja2 import Environment, FileSystemLoader
import yaml
import os



#Define airflow root folder
root_folder = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

#Name of template
template_name = 'F_template.jinja2'

#Destination for Dynamic Dag
DD_destination = os.path.join(root_folder, "dags")



#Current directory
file_dir = os.path.dirname(os.path.abspath(__file__))
#Get enviroment
env = Environment(loader=FileSystemLoader(file_dir))
#Get template
template = env.get_template(template_name)



for filename in os.listdir(file_dir):
    if filename.endswith('.yaml') and filename.startswith('F_config'):
        with open(f'{file_dir}/{filename}', 'r') as configfile:
            config = yaml.safe_load(configfile)
            DD_path = os.path.join(DD_destination,  f"{config['DD_file_name']}.py")
            with open(DD_path, 'w') as f:
                f.write(template.render(config))