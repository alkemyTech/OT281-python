import yaml
import os
from pathlib import Path
from jinja2 import FileSystemLoader, Environment

# Directory
file_dir = Path(__file__).parent

# Ariflow dir
parent_dir = Path(__file__).parent.parent.parent

# Enviroment
env = Environment(loader = FileSystemLoader(file_dir))

# Template
template = env.get_template('B_template.jinja2')

for file in os.listdir(file_dir):
    if file.endswith('.yaml'):
        with open(Path(file_dir, file), 'r') as configfile:
            # Load configurations
            config = yaml.safe_load(configfile)
            # Write python file  
            with open(Path(parent_dir, f'dags/{config["dag_id"]}.py'), 'w') as file:
                file.write(template.render(config))