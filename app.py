from flask import Flask

app = Flask(__name__)


@app.route("/")
def load_home():
    return "<a href='/log'>log</a>"


@app.route("/log")
def load_log():
    return "log!"
