from create_dataset import create_final_file
from insercao import criar_database
from os import path

if __name__ == "__main__":
    if not path.exists("final.feather"):
        create_final_file()
    
    criar_database()
