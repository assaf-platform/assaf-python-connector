# -*- coding: utf-8 -*-
import logging

from configloader import ConfigLoader
logging.basicConfig(level=0)



config = ConfigLoader()
config.update_from(yaml_file="config.yaml")

if __name__ == "__main__":
    logging.info(config)
