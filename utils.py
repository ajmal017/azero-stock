import configparser

from flask import Response
from dateutil.parser import parse


def get_config(section):
    config = configparser.ConfigParser()
    config.read('stock.ini')
    return config[section]


def parse_resp(status, content):
    return Response(content.to_json(orient='records'), mimetype='application/json') \
        if status == 0 and content is not None else Response(content, status=400)
