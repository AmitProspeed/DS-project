import socket
import requests
import platform
import sys
import os
import json
import signal
import jsonpickle
import io
import base64
import shutil
import itertools
import random

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

    #Send available chunks to another node having sufficient space randomnly selected
    #Get available nodes of the network
    server_url = 'http://' + redis_host + "/api/fetch/nodes"
    response = requests.get(server_url, verify=False)
    json_response = response.json()
    nodeData = json.loads(json_response['value'])
    ips = [node[0] for node in nodeData]
    spaces = [node[1] for node in nodeData]
    print('IPS: ', ips)
    print('Type: ', type(ips))
    # Remove the current node IP from this list
    current_node_ip = ip + ":" + port
    index = ips.index(current_node_ip)
    del ips[index]
    del spaces[index]
    #check space available in the peer
    available_ips = []
    disk_size = 0
    files = os.listdir('../output/')
    for f in files:
        if "_part" in f:
            fb = os.path.join('../output/', f)
            disk_size += os.path.getsize(fb)
    disk_size /= 2**20

    print ('Total size of available chunks:', disk_size)
    for i in ips:
        index = ips.index(i)
        available_space = spaces[index]
        if available_space > disk_size:
            available_ips.append(i)

    IP = random.choice(available_ips)

    #get the chunk files and send to the random IP selected
    for file in files:
        if "_part" in file:
            print ('chunk: ', file)
            filepath = os.path.join('../output/', file)
            with open (filepath, 'rb') as file_bytes:
                data = file_bytes.read()
                file_data = {'name':file, 'data':data.decode('utf-8')}
                print('IP: ', IP)
                server_url = 'http://' + IP + '/api/store_content'
                headers = {'content-type': 'application/pdf'}
                response = requests.post(server_url, data=json.dumps(file_data), headers=headers).json()
                print('Response: ', response)

            #Fetch all files present in redis and find the main file
            remote_url = 'http://' + redis_host + "/api/getAll"
            response = requests.get(remote_url, verify=False)
            json_response = response.json()
            print('JSON Response: ', json_response)
            #file_list_dict = fetchFiles()
            #print (file_list_dict)
            file_list = json_response['keys']
            print ('file list:', file_list)
            fname = file.split("_part")[0]
            chunk_number = file.split("_part")[1]
            chunk_number = int(chunk_number)
            for f in file_list:
                if fname in f:
                    filename = f.split("file:")[1]
                    break

            # Update Redis key-value Key: file:<file_name> Value: {IP: List(chunk_numbers)}
            server_url = 'http://' + redis_host + '/api/fetch/' + 'file:' + filename
            response = requests.get(server_url).json()
            print('Fetch Key Response: ', response)
            if response['value'] is None:
                value = {IP: [chunk_number]}
            else:
                value = json.loads(response['value'])
                chunk_list = None
                #remove the current chunk info
                current_chunk_list = value[current_node_ip]
                current_chunk_list.remove(chunk_number)
                value[current_node_ip] = current_chunk_list

                #set the updated chunk info
                if IP in value.keys():
                    chunk_list = value[IP]
                if IP not in value.keys() or chunk_list is None:
                    value[IP] = [chunk_number]
                else:
                    chunk_set = set(chunk_list)
                    chunk_set.add(chunk_number)
                    value[IP] = list(chunk_set)

            print('Final Updated Value: ', value)
            data = {'key': 'file:'+filename, 'value': json.dumps(value)}
            headers = {'Content-Type': 'application/json'}
            server_url = 'http://' + redis_host + '/api/set'
            response = requests.post(server_url, data=json.dumps(data), headers=headers).json()


    #call node leave API
    print('IP: ', ip)
    print('Port: ', port)
    print('Redis Host: ', redis_host)
    data = {
        'ip': ip + ':' + port,
    }
    remote_url = 'http://' + redis_host + "/api/leave"
    print('Remote url: ', remote_url)
    headers = {
        'Content-Type': 'application/json'  
    }
    try:
        leave_url = requests.post(remote_url, data=json.dumps(data), headers=headers, verify=False)
        print('Join URL: ', join_url.json())
        print('You pressed Ctrl+C!')
        sys.exit(0)
    except Exception as e:
        print ("Exception occurred:", e)
        raise Exception('Node leave failure for IP:', ip)

@app.route('/api/store', methods=['POST'])
def store():
    '''
        Store file in the P2P network
    '''    
    try:
        '''
            1. First, send a request to redis to get IP's of nodes with available storage
            2. Once the ips are received, if re_factor is 1, we do round robin file splits else we do random replicated file split assignments
        '''

        #Get the replication factor
        server_url = 'http://' + redis_host + "/api/fetch/re_factor"  
        response = requests.get(server_url, verify=False)
        json_response = response.json()

        re_factor = json.loads(json_response['value'])
        print ("Replication factor: ", re_factor)


        server_url = 'http://' + redis_host + "/api/fetch/nodes"  
        response = requests.get(server_url, verify=False)
        json_response = response.json()

        nodeData = json.loads(json_response['value'])
        ips = [node[0] for node in nodeData]
        spaces = [node[1] for node in nodeData]
        current_node_ip = ip + ":" + port

        print('IPS: ', ips)
        print('Type: ', type(ips))
        # Remove the current node IP from this list
        index = ips.index(current_node_ip)
        del ips[index]
        del spaces[index]

        if len(ips) == 0:
            return Response(response=jsonpickle.encode({ 'message': 'No nodes in the network', 'done': False }), status=200, mimetype="application/json")

        # Split the file in chunks and assign to peers in round robin order
        round_robin_ip = itertools.cycle(ips)
        chunksize = 1
        files = os.listdir('../input_files/')

        for file in files:
            filepath = os.path.join('../input_files/', file)
            partnum = 0
            file_data_list = []

            #storing the file chunks in a list
            with open (filepath, 'rb') as file_bytes: 
                while 1:
                    chunk = file_bytes.read(chunksize * (2**20))  # get next part <= chunksize
                    if not chunk: break
                    partnum = partnum + 1
                    filename = file.split(".")[0] + '_part%d' % partnum
                    file_data = {'name':filename, 'data':chunk.decode('utf-8')}
                    file_data_list.append(file_data)

            for file_data in file_data_list:  # eof=empty string from read

                #check space available in the peer
                server_url = 'http://' + redis_host + "/api/fetch/nodes"
                response = requests.get(server_url, verify=False)
                json_response = response.json()
                nodeData = json.loads(json_response['value'])
                available_ips = []
                for i in ips:
                    index = ips.index(i)
                    available_space = spaces[index]
                    if available_space > chunksize:
                        available_ips.append(i)

                if re_factor == 1:
                    IP = [next(round_robin_ip)]
                    index = ips.index(IP[0])
                    available_space = spaces[index]
                    while available_space < chunksize:
                        IP = [next(round_robin_ip)]
                        index = ips.index(IP[0])
                        available_space = spaces[index]
                else:
                    IP = random.sample(available_ips, min(len(available_ips),re_factor))        

                partnum = file_data_list.index(file_data) + 1    
                for i in IP:
                    print('IP: ', i)
                    server_url = 'http://' + i + '/api/store_content'
                    headers = {'content-type': 'application/pdf'}
                    response = requests.post(server_url, data=json.dumps(file_data), headers=headers).json()
                    print('Response: ', response)
                    # Update Redis key-value Key: file:<file_name> Value: {IP: List(chunk_numbers)}
                    server_url = 'http://' + redis_host + '/api/fetch/' + 'file:' + file
                    response = requests.get(server_url).json()
                    print('Fetch Key Response: ', response)
                    if response['value'] is None:
                        value = {i: [partnum]}
                    else:
                        value = json.loads(response['value'])
                        chunk_list = None
                        if i in value.keys():
                            chunk_list = value[i]
                        if i not in value.keys() or chunk_list is None:
                            value[i] = [partnum]
                        else:
                            chunk_set = set(chunk_list)
                            chunk_set.add(partnum)
                            value[i] = list(chunk_set)

                    print('Final Updated Value: ', value)
                    data = {'key': 'file:'+file, 'value': json.dumps(value)}
                    headers = {'Content-Type': 'application/json'}
                    server_url = 'http://' + redis_host + '/api/set'
                    response = requests.post(server_url, data=json.dumps(data), headers=headers).json()
            assert partnum <= 9999  # join sort fails if 5 digits

        return Response(response=jsonpickle.encode({ 'ips': ips, 'message': files }), status=200, mimetype="application/json")

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
        file_node_data = json.loads(response['value'])
        print('File Node Data: ', file_node_data)

        if file_node_data is None:
            print("No peer has this file in the network")
            return Response(response=jsonpickle.encode({ 'message': 'No peer has this file in the network'}))

        #Creating a dictionary to store chunk id's as keys in sorted order with peer IP as value
        chunk_list = dict()

        for k, v in file_node_data.items():
            for id in v:
                if id not in chunk_list.keys():
                    chunk_list[id] = [k]
                else:
                    ip_list = chunk_list[id]
                    ip_list.append(k)
                    chunk_list[id] = ip_list

        #Merging chunks from respective peers in sorted order and regenerating the file
        path = os.path.join('../output/', file_name.split(":")[1])
        with open(path, 'wb') as f:
            for i in sorted(chunk_list.keys()):
                filename = file_name.split(":")[1].split(".")[0] + '_part' + str(i)
                print('Filename: ', filename)
                for j in chunk_list[i]:
                    try:
                        response = requests.get('http://' + j + '/api/fetch_content/' + filename).json()
                        res = bytes(response['py/b64'], 'utf-8')
                        f.write(base64.decodebytes(res))
                        break
                    except Exception as e:
                        if j == chunk_list[i][-1]:
                            raise Exception('missing chunk:', i)
                        else: 
                            continue                        

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
    print ('Receiving Bytes')
    r = jsonpickle.decode(request.data.decode('utf-8'))
    try:
        name = r['name']
        data = r['data']
        path = os.path.join('../output/',name)
        #remove if same chunk is present
        if (os.path.exists(path)):
            os.remove(path)
        with open(path, 'wb') as fileobj:
            fileobj.write(data.encode('utf-8'))

        #update available disk space in redis
        current_node_ip = ip + ":" + port
        total, used, free = shutil.disk_usage("/")  #calculated space is in bytes
        print('Free space: ', free)
        data = {
            'ip': current_node_ip,
            'space': free // (2**20)    #converting to MB    
        }
        remote_url = 'http://' + redis_host + "/api/join"
        print('Remote url: ', remote_url)
        headers = {
            'Content-Type': 'application/json'
        }
        join_url = requests.post(remote_url, data=json.dumps(data), headers=headers, verify=False)
        print(join_url.json())

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
        total, used, free = shutil.disk_usage("/")  #calculated space is in bytes
        print ('Free space: ', free // (2**20))
        data = {
            'ip': ip + ':' + port,
            'space': free // (2**20)    #converting bytes to MB
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
        app.run(host="0.0.0.0",port=port)
        print('Coordinator Server started!')
        signal.pause()
    except Exception as e:
        print('Unable to fetch hostname and IP')
        print('Exception trace: ', e)
