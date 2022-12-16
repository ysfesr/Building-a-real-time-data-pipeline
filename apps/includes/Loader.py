import yaml
from configparser import ConfigParser
import os.path
from abc import ABC, abstractmethod

class Loader(ABC):

    def __init__(self, path:str) -> None:
        self.path = path
        self.read_file()
        self.data = []
        self.load()


    @abstractmethod
    def load(self):
        pass


    def read_file(self) -> None:
        self.stream = open(os.path.dirname(__file__) + "/../" + self.path, 'r')


    def get_data(self)->list or dict:
        return self.data


    
class YamlLoader(Loader):

    def load(self):
        self.data = yaml.safe_load(self.stream)


        
class IniLoader(Loader):

    def read_file(self) -> None:
        self.config = ConfigParser()
        self.config.read(os.path.dirname(__file__) + "/../" + self.path)


    def load(self):
        self.data = {x:{**y} for x,y in self.config.items()}
