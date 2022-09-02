#Imports
from jinja2 import Environment, FileSystemLoader
import yaml
import os

#Template path
TEMPLATE_PATH = "D_template.jinja2"
#Get the root folder (project folder)
ROOT_FOLDER = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

#Get file directory
file_dir = os.path.dirname(os.path.abspath(__file__))
#Get enviroment
env = Environment(loader=FileSystemLoader(file_dir))
#Get template
template = env.get_template(TEMPLATE_PATH)

#Iterate through files in file directory
for filename in os.listdir(file_dir):
    #Open only the .yaml files (as read mode)
    if filename.endswith(".yaml"):
        with open(f"{file_dir}/{filename}", "r") as configfile:
            #Load the configuration from the .yaml file
            config = yaml.safe_load(configfile)
            #Create a new DAG in the "dags" folder
            new_dag_path = os.path.join(ROOT_FOLDER, 'dags', f"{config['DD_file_name']}.py")
            with open(new_dag_path, "w") as f:
                #Write the DAG based on the template and the current configuration
                f.write(template.render(config))