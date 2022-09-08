from jinja2 import Environment, FileSystemLoader
import yaml 
import os



################## Path #############################
FILE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_FOLDER = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
FILE_NAME = 'H_template.jinja2'
FILENAME_PREFIX = "H_config"
################## GET ENV #############################
env = Environment(loader=FileSystemLoader(FILE_DIR))
################## GET TEMPLATE #############################
template = env.get_template(FILE_NAME)

################## CREATE FILE IN DIRECTORY ################
for filename in os.listdir(FILE_DIR):
     if filename.endswith(".yaml") and filename.startswith(FILENAME_PREFIX):
         with open(f'{FILE_DIR}/{filename}', 'r') as configfile:
             config = yaml.safe_load(configfile)
             pad_new_dag = os.path.join(ROOT_FOLDER,'dags', f"{config['DD_file_name']}.py")
             with open (pad_new_dag, 'w') as f:
                 f.write(template.render(config))