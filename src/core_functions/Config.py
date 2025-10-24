import os
from typing import Dict
from core_functions.utils import read_yaml, project_path


class Config:
    """
    This is a class that retrieves configuration information for a given entity from a yaml file.
    """

    def __init__(self, yaml_config: str, yaml_key: str, key_name: str, logger):
        """
        Constructor for the Config class

        Input:
            - yaml_config : str, The path to the yaml config file
            - yaml_key : str, The key to retrieve from the yaml file
            - key_name : str, The name of the key to add to the configuration
            - logger : Logger object for logging operations (required)
        Output:
            - None
        """
        if logger is None:
            raise ValueError("Logger is required for Config class")
            
        self.yaml_config = yaml_config
        self.yaml_key = yaml_key
        self.key_name = key_name
        self.logger = logger
        self.retrieved_config = self._get_config_from_yaml()

    def _get_config_from_yaml(self) -> Dict:
        """
        Retrieves the configuration from the specified YAML file.

        Input:
            - None
        Output:
            - Dict : The configuration dictionary retrieved from the YAML file
        """
        self.logger.info(f"Loading configuration from YAML file: {self.yaml_config}")
        self.logger.debug(f"Looking for key: {self.yaml_key}")
        
        project_file_path = project_path()
        path_to_config = os.path.join(project_file_path, self.yaml_config)
        
        self.logger.debug(f"Full config file path: {path_to_config}")
        
        data = read_yaml(path_to_config)

        data = data.get(self.yaml_key, None)
        if not data:
            error_msg = f"Key '{self.yaml_key}' not found in the specified config file. Ensure the key exists in the YAML file."
            self.logger.error(error_msg)
            raise KeyError(error_msg)
            
        if not self.key_name:
            error_msg = f"Key Name '{self.key_name}' is None. A valid key name is required to add to the configuration."
            self.logger.error(error_msg)
            raise KeyError(error_msg)

        data[self.key_name] = self.yaml_key

        self.logger.info(f"Successfully loaded configuration for key: {self.yaml_key}")
        self.logger.debug(f"Configuration keys: {list(data.keys())}")

        return data

    def get_config(self) -> Dict:
        """
        Returns the retrieved configuration

        Input:
            - None
        Output:
            - Dict : The retrieved configuration dictionary
        """
        self.logger.debug("Retrieving configuration")
        return self.retrieved_config
