from jinja2 import Environment,FileSystemLoader
import yaml
import os

file_dir = os.path.dirname(os.path.abspath(__file__))
env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_template('C_template.jinja2')

for filename in os.listdir(file_dir):
        if filename.endswith(".yaml") and filename.startswith("C_"):
            with open(f"{file_dir}/{filename}","r")as configfile:
                config = yaml.safe_load(configfile)
                with open(f"{file_dir}/../../dags/{config['dag_id']}.py","w") as f:
                    f.write(template.render(config))