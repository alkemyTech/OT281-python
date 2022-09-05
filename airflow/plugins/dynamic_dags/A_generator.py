from re import template
from jinja2 import Environment, FileSystemLoader
import yaml
import os

file_dir = os.path.dirname(os.path.abspath(__file__))
env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_template('A_template.jinja2')

for filename in os.listdir(file_dir):
    if filename.endswith('.yaml'):
        with open(f'{file_dir}/{filename}', 'r') as configfile:
            config = yaml.safe_load(configfile)
            with open(f"dags/A_DD_{config['dag_id']}.py", 'w') as f:
                f.write(template.render(config))