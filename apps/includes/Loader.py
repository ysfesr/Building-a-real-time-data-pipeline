import yaml
<<<<<<< HEAD
import csv
import os.path
from configparser import ConfigParser
from abc import ABC, abstractmethod


class Loader(ABC):

    """
        Loader class to inherit from to create custom loader.
        Its goal is to abstract the loading process from the actual spark app.
        This class is only made to be inherited from, 
            it will raise an error if implemented directly.
    """

    def __init__(self, path:str) -> None:
        self.path = path
        self.data = []
        self.read_file()
=======
from configparser import ConfigParser
import os.path
from abc import ABC, abstractmethod

class Loader(ABC):

    def __init__(self, path:str) -> None:
        self.path = path
        self.read_file()
        self.data = []
        self.load()
>>>>>>> c88d118f940fa44c615524f06d5ed6628bf40664


    @abstractmethod
    def load(self):
<<<<<<< HEAD
        """
            Implements the "self.data" variable which will contain
                the file data in the desired format.
            Has access to the file stream by using "self.stream".
        """
=======
>>>>>>> c88d118f940fa44c615524f06d5ed6628bf40664
        pass


    def read_file(self) -> None:
<<<<<<< HEAD
        """
            The function that does the loading,
            Use "self.load()" to load the data and make it accessible
        """
        with open(os.path.dirname(__file__) + "/../" + self.path, 'r') as self.stream:
            self.load()
=======
        self.stream = open(os.path.dirname(__file__) + "/../" + self.path, 'r')
>>>>>>> c88d118f940fa44c615524f06d5ed6628bf40664


    def get_data(self)->list or dict:
        return self.data


<<<<<<< HEAD
=======
    
>>>>>>> c88d118f940fa44c615524f06d5ed6628bf40664
class YamlLoader(Loader):

    def load(self):
        self.data = yaml.safe_load(self.stream)


<<<<<<< HEAD
=======
        
>>>>>>> c88d118f940fa44c615524f06d5ed6628bf40664
class IniLoader(Loader):

    def read_file(self) -> None:
        self.config = ConfigParser()
        self.config.read(os.path.dirname(__file__) + "/../" + self.path)
<<<<<<< HEAD
        
        self.load()
=======
>>>>>>> c88d118f940fa44c615524f06d5ed6628bf40664


    def load(self):
        self.data = {x:{**y} for x,y in self.config.items()}
<<<<<<< HEAD


class CsvLoader(Loader):
    
    def load(self) -> list:
        self.data = list(csv.DictReader(self.stream))
=======
>>>>>>> c88d118f940fa44c615524f06d5ed6628bf40664
