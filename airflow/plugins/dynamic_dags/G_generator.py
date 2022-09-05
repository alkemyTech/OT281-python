from jinja2 import Environment, FileSystemLoader
import yaml
import os

#file directory
file_dir = os.path.dirname(os.path.abspath(__file__))
env = Environment(loader=FileSystemLoader(file_dir))
#get the template
template = env.get_template('G_template.jinja2')

#for loop to read all config_G files and create all G dynamics Dags
for filename in os.listdir(file_dir):
        #select only files that end with ".yaml" and stat with "config_G"
        if filename.endswith(".yaml") and filename.startswith("config_G"):
            with open(f"{file_dir}/{filename}","r")as configfile:
                config = yaml.safe_load(configfile)
                #create a new dag
                with open(f"{file_dir}/../../dags/{config['dag_id']}.py","w") as f:
                    f.write(template.render(config))