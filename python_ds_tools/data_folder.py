import os

#Retrieve the corresponding local data folder given a file
def for_file(path):
    #Root folder for the project is
    root_folder = os.environ['ROOT_FOLDER']
    #Data input/output is expected in
    data_folder = os.environ['DATA_FOLDER']
    script_dir = os.path.abspath(os.path.dirname(path))
    rel_to_root = os.path.relpath(script_dir, root_folder)
    rel_to_data = os.path.join(data_folder, rel_to_root)
    return rel_to_data

#This function uses the 'ROOT_FOLDER' and 'DATA_FOLDER'
#and creates the same tree structure as in the code, but in the data folder
#so the user can start putting the raw data in the correct places

def setup():
    root_folder = os.environ['ROOT_FOLDER']

    #Abs directories
    abs_directories = [x[0] for x in os.walk(root_folder)]
    #Get relative directories
    rel_directories = [os.path.relpath(abs_dir, root_folder) for abs_dir in abs_directories]

    #Now join relative directories with DATA_FOLDER
    data_folder = os.environ['DATA_FOLDER']
    abs_data_directories = [os.path.join(data_folder, rel_dir) for rel_dir in rel_directories]

    #Create folders if they dont exist
    for abs_data_dir in abs_data_directories:
        if not os.path.exists(abs_data_dir):
            os.makedirs(abs_data_dir)
    #Maybe add a .gitignore-like file, its not useful to
    #create directories that are not going to create any output