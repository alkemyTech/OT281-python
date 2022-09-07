"""
    This script generates Dynamic DAGs for Group D Universities
    (Universidad Tecnológica Nacional and Universidad Nacional de Tres de Febrero).
"""

#Imports
from jinja2 import Environment, FileSystemLoader
import yaml
import os

#Get the root folder (project folder)
ROOT_FOLDER = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
#Template path (.jinja2 file)
TEMPLATE_PATH = "D_template.jinja2"
#Folder where store generated Dynamic Dags
DD_OUTPUT_FOLDER = os.path.join(ROOT_FOLDER, "dags")
#Config filename prefix (used to get the right config files)
CONFIG_FILENAME_PREFIX = "D_config"
#Config file parameter that defines the DD filename
DD_FILENAME_PARAMETER = "DD_file_name"

#Get file directory
file_dir = os.path.dirname(os.path.abspath(__file__))
#Get enviroment
env = Environment(loader=FileSystemLoader(file_dir))
#Get template
template = env.get_template(TEMPLATE_PATH)

#Methods
def generate_dynamic_dags(file_dir, template, output_folder, DD_filename_parameter, config_filename_prefix=None):
    """
        This method generates dynamic DAGs (DD) in 'output_folder' given certain arguments.
        NOTE: This method assumes that config files have a parameter that defines the output DD filename.

    Args:
        file_dir (str): Directory where search for .yaml config files.
        template (str): Path of the .jinja2 template to use.
        output_folder (str): Path of the folder where DD will be stored in.
        DD_filename_parameter (str): Parameter of .yaml config files that defines the filename of each DD.
        config_filename_prefix (str, optional): .yaml config filenames need to match this prefix in order to be used. Defaults to None.
    """
    #Iterate through files in file directory
    for filename in os.listdir(file_dir):
        #Open only the .yaml files that starts with CONFIG_FILENAME_PREFIX (as read mode)
        if filename.endswith(".yaml"):
            #If config_filename_prefix is given, use it to filter config files
            if config_filename_prefix == None or filename.startswith(CONFIG_FILENAME_PREFIX):
                with open(f"{file_dir}/{filename}", "r") as configfile:
                    #Load the configuration from the .yaml file
                    config = yaml.safe_load(configfile)
                    #Get the filename from this Dynamic Dag (stored in the config file)
                    DD_filename = f"{config[DD_filename_parameter]}.py"
                    #Create a new DAG in the output folder
                    new_dag_path = os.path.join(output_folder, DD_filename)
                    with open(new_dag_path, "w") as f:
                        #Write the DAG based on the template and the current configuration
                        f.write(template.render(config))

#This is called when D_generator.py is run directly
if __name__ == "__main__":
    #Generate DDs with defined arguments
    generate_dynamic_dags(
        file_dir,
        template,
        DD_OUTPUT_FOLDER,
        DD_FILENAME_PARAMETER,
        config_filename_prefix=CONFIG_FILENAME_PREFIX
        )