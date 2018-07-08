from flask import Flask, render_template
from market import market_app
from utils import *

app = Flask(__name__)
app.register_blueprint(market_app)


@app.route('/')
def hello():
    return render_template('index.html')


if __name__ == "__main__":
    cfg = get_config('server')
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.run(threaded=True, debug=True, host=cfg['ServerHost'], port=int(cfg['ServerPort']))
