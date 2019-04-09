import os
from configparser import ConfigParser


def get_config(section, key):
    config = ConfigParser()
    path = os.path.split(os.path.realpath(__file__))[0] + '\..\conf\env.conf'
    config.read(path)
    return config.get(section, key)
