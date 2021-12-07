#!/usr/bin/python3

import sys
import socket
import threading
import pickle
import struct
import implement.client as Client
import time
PORT = None
CLIENT_ID = None
LOCAL_HOST_NAME = "127.0.0.1"
CONNECTION = None
IN_CHANNELS = {}
OUT_CHANNELS = {}

IS_CONNECTED_TO_ALL_NODES = False
IS_LISTENING_TO_ALL_NODES = False



def start_client():
    global CLIENT_ID, CONNECTION
    CLIENT_ID, config_file = check_cl_args()
    serverInfos = read_config_file(config_file)
    coordinator = serverInfos[0]
    print(coordinator)
    s = socket.socket()
    s.connect((coordinator[1], int(coordinator[2])))
    message = {"node_id": CLIENT_ID}
    s.sendall(pickle.dumps(message))
    CONNECTION = s
    client = Client.Client(CLIENT_ID, send_message)
    # nodeNumber = len(nodeInfos)
    # lock = threading.Lock()
    threading.Thread(target=user_input_handler, args=(client,)).start()
    # threading.Thread(target=listen_to_services, args=(nodeNumber,)).start()


def user_input_handler(client):
    while True:
        userInput = input()
        client.userInput(userInput)




def send_message(data):
    message = {"client_id": CLIENT_ID, "content": data}
    toSendData = pickle.dumps(message)
    header = struct.pack('i', len(toSendData))
    try:
        CONNECTION.sendall(header + toSendData)
    except Exception:
        pass


def listening_server(connection, client):
    while True:
        received_time = time.time()
        header_struct = connection.recv(4)
        if len(header_struct) < 1:
            print(received_time, "-", client.clientId, "disconnected")
            # TotalOrdering.nodeFailed(nodeID)
            return
        unpack_res = struct.unpack('i',header_struct)
        size = unpack_res[0] 
        recv_size = 0
        total_data = b''
        while recv_size < size:
            recv_data = connection.recv(size - recv_size)
            if len(recv_data) < 1:
                print(received_time, "-", client.clientId, "disconnected")
                return 
            recv_size += len(recv_data)
            total_data += recv_data
        message = receive_server_message(total_data)
        if message:
            print("from server: ", message)
            client.receiveMessage(message)



def receive_server_message(raw_data):
    message = pickle.loads(raw_data) 
    return message["content"]


def server_unicast(message, connection):
    toSendData = pickle.dumps(message)
    header = struct.pack('i', len(toSendData))
    try:
        connection.sendall(header + toSendData)
    except Exception:
        pass


def connect_to_services(serverInfos):
    for serverInfo in serverInfos:
        threading.Thread(target=connect_to_server, args=(serverInfo,)).start()

    while len(OUT_CHANNELS) != len(serverInfos):
        continue
    global IS_CONNECTED_TO_ALL_NODES
    IS_CONNECTED_TO_ALL_NODES = True
    while not IS_CONNECTED_TO_ALL_NODES or not IS_LISTENING_TO_ALL_NODES:
        continue
    print("Connected to all nodes")
    while True:
        # call server services to communicate between servers
        message = input()
        multicast_message(message)

def connect_to_server(serverInfo):
    s = socket.socket()
    while True:
        try:
            s.connect((serverInfo[1], int(serverInfo[2])))
            message = {"node_id": CLIENT_ID, "port": PORT}
            OUT_CHANNELS[serverInfo[0]] = s
            s.sendall(pickle.dumps(message))
            break
        except Exception:
            pass
    return


def listen_to_services(servicesNumber):
    pass


def check_cl_args():
    if len(sys.argv) != 3:
        print("2 arguments needed: Client_id CONFIG_FILE")
        sys.exit()
    return sys.argv[1:]


def read_config_file(config_file):
    global CLIENT_ID, PORT, SERVER_NAMES
    f = open(config_file, "r")
    nodeInfos = []
    configInfo = f.readline()

    while configInfo:
        configInfo = configInfo.split(" ")
        configInfo[2] = configInfo[2].strip()
        if configInfo[0] == CLIENT_ID:
            PORT = configInfo[2]
        else:
            nodeInfos.append(configInfo)
        configInfo = f.readline()
    return nodeInfos



if __name__ == "__main__":
    start_client()