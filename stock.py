from flask import Flask
from market import market_app
from utils import *

app = Flask(__name__)
app.register_blueprint(market_app)

if __name__ == "__main__":
    cfg = get_config('server')
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.run(debug=True, host=cfg['ServerHost'], port=int(cfg['ServerPort']))
