from flask import Flask, render_template
from flask_redis import FlaskRedis
app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello World! go go go!'

@app.route('/test')
def call_redis():
    REDIS_URL = "redis://localhost:6379/0"
    redis_client = FlaskRedis(app)
    keys = [k.decode('utf-8') for k in redis_client.keys('*')]
    dic = {k:int(redis_client.get(k).decode('utf-8')) for k in keys}
    ret = [{k:v} for k,v in sorted(dic.items(), key= lambda item: item[1], reverse=True)][:10]
    return str(ret)

@app.route('/chart')
def show_chart():
    REDIS_URL = "redis://localhost:6379/0"
    redis_client = FlaskRedis(app)
    keys = [k.decode('utf-8') for k in redis_client.keys('*')]
    d = {k:int(redis_client.get(k).decode('utf-8')) for k in keys}
    dd = [{k:v} for k,v in sorted(d.items(), key= lambda item: item[1], reverse=True)][:30]

    value = []
    for i in dd:
        for k,v in i.items():
            value += [v]
    k = [k for k,v in i.items() for i in dd] 

    key = []
    for i in dd:
        for k,v in i.items():
            key += [k]
    series = [{'name': 'tweet', 'data': [v for i in dd for k,v in i.items()] }]
    title = {'text': 'Top 30 Altcoin on Tweeter crypto keyword'}
    xAxis = [k for i in dd for k,v in i.items()]
    yAxis = {'title':{'text': 'mention'}}
    return render_template('index.html', series=series, title=title, xAxis=xAxis, yAxis=yAxis)

@app.route('/profile/<username>')
def get_profile(username):
    return 'profile : ' + username

