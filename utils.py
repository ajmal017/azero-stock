import configparser


def get_config(section):
    config = configparser.ConfigParser()
    config.read('stock.ini')
    return config[section]
