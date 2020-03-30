from flask import Flask, request, Response
import jsonpickle

from redis_wrapper.main import RedisWrapper

global redis_object
app = Flask(__name__)

@app.route("/api/ping", methods=['POST'])
def hello():
    response = jsonpickle.encode({ 'ping': 'pong' })
    return Response(response=response, status=200, mimetype="application/json")

@app.route("/api/getAll", methods=['GET'])
def keys():
    '''
        Return keys
    '''
    try:
        response = {
            keys: []
        }
        return Response(response=jsonpickle.encode(response), status=200, mimetype="application/json")
    except Exception as e:
        response = {
            trace: e,
            error: True
        }
        return Response(response=jsonpickle.encode(response), status=500, mimetype="application/json")

@app.route("/api/fetch/<key>", methods=['GET'])
def fetchKey(key):
    '''
        Fetch a particular key in Redis
    '''
    try:
        response = {
            key: '',
            value: ''
        }
        return Response(response=jsonpickle.encode(response), status=200, mimetype="application/json")
    except Exception as e:
        response = {
            trace: e,
            error: True
        }
        return Response(response=jsonpickle.encode(response), status=500, mimetype="application/json")

@app.route('/api/set', methods=['POST'])
def setKeyValue():
    '''
        Set Key-Value Pair
    '''    
    try:
        key = request.form['key']
        value = request.form['value']

        response = {
            status: True
            key: key,
            value: value
        }
        return Response(response=jsonpickle.encode(response), status=200, mimetype="application/json")
    except Exception as e:
        response = {
            trace: e,
            error: True
        }
        return Response(response=jsonpickle.encode(response), status=500, mimetype="application/json")


@app.route('/api/join', methods=['POST'])
def nodeJoin():
    '''
        A Node joins the network
        Input: IP
        TODO: In future, stats about what resources this node can offer
    '''
    pass



if __name__ == "__main__":
    redis_object = RedisWrapper("0.0.0.0", "6379")
    app.run()