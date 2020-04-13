import socket
import requests
import platform
import sys
import json
import signal
import jsonpickle
import io
import base64

from flask import Flask, request, Response

app = Flask(__name__)
global redis_host
global ip
global port

def signal_handler(sig, frame):
    '''
        Handle Nodes that leave the network
        Prune and cleanup
    '''
    # Implement Node Leave here!
    print('You pressed Ctrl+C!')
    sys.exit(0)

@app.route('/api/store', methods=['POST'])
def store():
    '''
        Store file in the P2P network
    '''    
    try:
        '''
            1. First, send a request to redis to get IP's of nodes with available storage
            2. Once the ips are received choose one at random(have to change this logic) and send the file to that node 
        '''

        server_url = 'http://' + redis_host + "/api/fetch/nodes"  
        response = requests.get(server_url, verify=False)
        json_response = response.json()

        ips = json.loads(json_response['value'])
        current_node_ip = ip + ":" + port

        print('IPS: ', ips)
        print('Type: ', type(ips))
        # Remove the current node IP from this list
        ips.remove(current_node_ip)

        if len(ips) == 0:
            return Response(response=jsonpickle.encode({ 'message': 'No nodes in the network', 'done': False }), status=200, mimetype="application/json")
        
        # Choose one from the remaining list if any
        IP = ips[0]
        print('IP: ', IP)
        # Send a request to this IP to store the file
        server_url = 'http://' + IP + '/api/store_content'
        headers = {'content-type': 'application/pdf'}
        file_bytes = open('../input_files/sample.pdf', 'rb').read()
        response = requests.post(server_url, data=file_bytes, headers=headers).json()
        print('Response: ', response)

        # Update Redis key-value Key: file:<file_name> Value: List(IP)
        server_url = 'http://' + redis_host + '/api/fetch/' + 'file:sample.pdf'
        response = requests.get(server_url).json()
        print('Fetch Key Response: ', response)
        if response['value'] is None:
            value = [IP]
        else:
            value = json.loads(response['value'])
            value.append(IP)
        
        print('Final Updated Value: ', value)
        data = { 'key': 'file:sample.pdf', 'value': json.dumps(value) }
        headers = { 'Content-Type': 'application/json' }
        server_url = 'http://' + redis_host + '/api/set'
        response = requests.post(server_url, data=json.dumps(data), headers=headers).json()

        return Response(response=jsonpickle.encode({ 'ips': ips, 'message': response }), status=200, mimetype="application/json")

    except Exception as e:
        response = {
            'trace': e,
            'error': True
        }
        return Response(response=jsonpickle.encode(response), status=500, mimetype="application/json")


@app.route('/api/fetch/<file_name>', methods=['GET'])
def fetch(file_name):
    '''
        Fetch a particular file to disk
    '''
    try:    
        server_url = 'http://' + redis_host + "/api/fetch/" + file_name
        response = requests.get(server_url, verify=False).json()
        print('JSON Response: ', response)
        file_node_ip = json.loads(response['value'])
        print('File Node IP: ', file_node_ip)

        if file_node_ip is None:
            print("No peer has this file in the network")
            return Response(response=jsonpickle.encode({ 'message': 'No peer has this file in the network'}))

        # 1. Send a request to this node to get file bytes and store in files directory
        IP = file_node_ip[0]
        filename = file_name.split(":")[1]
        print('Filename: ', filename)
        response = requests.get('http://' + IP + '/api/fetch_content/' + filename).json()
        print('Response: ', response)
        res = bytes(response['py/b64'], 'utf-8') 
        with open('../output/sample1.pdf', 'wb') as f:
            f.write(base64.decodebytes(res))
        # 2. Send a request to redis server again updating the value for this particular file.

        return Response(response=jsonpickle.encode({ 'message': 'File downloaded to disk' }), status=200, mimetype="application/json")
    except Exception as e:
        response = {
            'trace': e,
            'error': True
        }
        return Response(response=jsonpickle.encode(response), status=500, mimetype="application/json")

@app.route('/api/files', methods=['GET'])
def fetchFiles():
    '''
        Fetch all files that are available in the hash table
    '''
    try:    
        server_url = 'http://' + redis_host + "/api/getAll"
        response = requests.get(server_url, verify=False)
        json_response = response.json()
        print('JSON Response: ', json_response)
        return Response(response=jsonpickle.encode({ 'keys': json_response['keys']}), status=200, mimetype="application/json")
    except Exception as e:
        response = {
            'trace': e,
            'error': True
        }
        return Response(response=jsonpickle.encode(response), status=500, mimetype="application/json")

@app.route('/api/store_content', methods=['POST'])
def receive_content():
    '''
        Receive file bytes from peer and create a new file locally
    '''
    r = request
    try:
        print('Received Bytes')
        with open('../output/sample.pdf', 'wb') as f2:
            f2.write(r.data)
            return Response(response=jsonpickle.encode({'operation': 'File Written Successfully'}), status=200, mimetype="application/json")
    except Exception as e:
        response = {
            'trace': e,
            'error': True
        }
        return Response(response=jsonpickle.encode(response), status=500, mimetype="application/json")

@app.route('/api/fetch_content/<file_name>', methods=['GET'])
def fetch_content(file_name):
    '''
        Fetch File content from peer
    '''
    r = request
    try:
        print('Send Bytes')
        file_bytes = open('../output/' + file_name, 'rb').read()
        data = {
            'data': file_bytes
        }
        headers = {'content-type': 'application/pdf'}
        return Response(response=jsonpickle.encode(file_bytes), status=200, headers=headers, mimetype="application/json")
    except Exception as e:
        response = {
            'trace': e,
            'error': True
        }
        return Response(response=jsonpickle.encode(response), status=500, mimetype="application/json")

if __name__ == "__main__":
    try:
        print('PLatform Node: ', platform.node())
        ip = sys.argv[1]
        redis_host = sys.argv[2]
        port = sys.argv[3]
        print('IP: ', ip)
        print('Redis Host: ', redis_host)
        data = {
            'ip': ip + ':' + port
        }
        remote_url = 'http://' + redis_host + "/api/join"
        print('Remote url: ', remote_url)
        headers = {
            'Content-Type': 'application/json'  
        }
        join_url = requests.post(remote_url, data=json.dumps(data), headers=headers, verify=False)
        print('Join URL: ', join_url.json())

        
        signal.signal(signal.SIGINT, signal_handler)
        print('Press Ctrl+C to stop the process, sigint handler registered')
        print('Coordinator Server started!')
        app.run(port=port)
        print('Coordinator Server started!')
        signal.pause()
    except Exception as e:
        print('Unable to fetch hostname and IP')
        print('Exception trace: ', e)