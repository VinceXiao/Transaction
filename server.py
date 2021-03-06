#!/usr/bin/python3

import sys
import socket
import threading
import pickle
import struct
import time
import math
from implement import coordinator, server


IN_CHANNELS = {}
OUT_CHANNELS = {}
CLIENT_CHANNELS = {}
IS_CONNECTED_TO_ALL_NODES = False
IS_LISTENING_TO_ALL_NODES = False
IS_COORDIANTOR = False
COORDIANTOR_ID = None
NODE_ID = None
NODE_INT_ID = None
PORT = None
Coordinator = None
Server = None
LOCAL_HOST_NAME = '0.0.0.0'
MAX_NODE_COUNT = 50
timestamp = 0
message_ID_set = []
TotalOrdering = None
HEADER_SIZE = 5


node_lock = threading.Lock()

def start_node():
    global NODE_ID, NODE_NUMBER_ID, PORT, TotalOrdering, Coordinator, Server
    NODE_ID, config_file = check_cl_args()
    NODE_NUMBER_ID = str(hash(NODE_ID) % (10 ** 8))
    nodeInfos = read_config_file(config_file)
    nodeNumber = len(nodeInfos)
    lock = threading.Lock()
    Server = server.Server(NODE_ID, COORDIANTOR_ID, send_message_to_coordinator)
    if IS_COORDIANTOR:
        Coordinator = coordinator.Coordinator(NODE_ID, None, send_message_to_server, client_unicast)
    # TotalOrdering = total_ordering.TotalOrdering(unicast, multicast_message, NODE_ID, NODE_NUMBER_ID, nodeNumber, lock, record_message_time)
    threading.Thread(target=listen_to_nodes, args=(nodeNumber,)).start()
    threading.Thread(target=connect_to_nodes, args=(nodeInfos,)).start()
   
    #     threading.Thread(target=coordinator.)

def check_cl_args():
    if len(sys.argv) != 3:
        print("2 arguments needed: Node_Name Config_File")
        sys.exit()
    return sys.argv[1:]

def read_config_file(config_file):
    global PORT, IS_COORDIANTOR, COORDIANTOR_ID
    f = open(config_file, "r")
    nodeInfos = []
    configInfo = f.readline()
    isFirstLine = True
    while configInfo:
        configInfo = configInfo.split(" ")
        configInfo[2] = configInfo[2].strip()
        if configInfo[0] == NODE_ID:
            PORT = configInfo[2]
            if isFirstLine:
                IS_COORDIANTOR = True
                COORDIANTOR_ID = configInfo[0]
        else:
            nodeInfos.append(configInfo)
            if isFirstLine:
                COORDIANTOR_ID = configInfo[0]
        isFirstLine = False
        configInfo = f.readline()
    return nodeInfos

def connect_to_nodes(nodeInfos):
    for nodeInfo in nodeInfos:
        threading.Thread(target=connect_to_node, args=(nodeInfo,)).start()

    while len(OUT_CHANNELS) != len(nodeInfos):
        continue
    global IS_CONNECTED_TO_ALL_NODES
    IS_CONNECTED_TO_ALL_NODES = True
    while not IS_CONNECTED_TO_ALL_NODES or not IS_LISTENING_TO_ALL_NODES:
        continue
# all nodes connection setted up
# use isis to send user input messages
    # TotalOrdering.ProposeMessages()
    while True:
        data = input()
        multicast_server_message(data)


def connect_to_node(nodeInfo):
    s = socket.socket()
    while True:
        try:
            s.connect((nodeInfo[1], int(nodeInfo[2])))
            message = {"node_id": NODE_ID,}
            OUT_CHANNELS[nodeInfo[0]] = s
            s.sendall(pickle.dumps(message))
            break
        except Exception:
            pass
    return

def multicast_server_message(message, isFirstMulticast = True):
    global message_ID_set
    toSendPackage = message
    if isFirstMulticast:
        global timestamp
        toSendPackage = {"node_id": NODE_ID, "node_number_id": int(str(timestamp) + NODE_NUMBER_ID), "content": message}
        timestamp += 1
        message_ID_set.append(toSendPackage["node_number_id"])
    node_lock.acquire()
    for node in OUT_CHANNELS:
        server_unicast(toSendPackage, node)
    node_lock.release()

def send_message_to_server(message, serverId):
    if serverId == NODE_ID:
        # print("125 send message to server", serverId)
        Server.receiveServerMessage(message)
    else:
        print("128 send message to server", serverId)
        server_unicast(message, serverId)
        
def send_message_to_coordinator(message):
    if NODE_ID == COORDIANTOR_ID:
        Coordinator.receiveServerMessage(message)
    else:
        server_unicast(message, COORDIANTOR_ID)

def server_unicast(message, nodeId, isUnicast = False):
    global graph_data
    if isUnicast:
        message["isUnicast"] = True
    toSendData = pickle.dumps(message)
    header = struct.pack('i', len(toSendData))
    try:
        OUT_CHANNELS[nodeId].sendall(header + toSendData)
    except Exception:
        pass

def client_unicast(message, clientId):
    toSendData = pickle.dumps(message)
    header = struct.pack('i', len(toSendData))
    try:
        CLIENT_CHANNELS[clientId].sendall(header + toSendData)
    except Exception:
        pass

def receive_server_message(client_data):
    global message_ID_set
    message = pickle.loads(client_data) 
    info = None
    if "node_number_id" not in message:
        info = message
    elif "isUnicast" in message:
        info = message["content"]
    elif message["node_number_id"] in message_ID_set:
        info = None
    else:
        message_ID_set.append(message["node_number_id"])
        multicast_server_message(message, False)
        info = message["content"]
    return info

def listen_to_nodes(expectedNodeNumber):
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 32*1024)
    print(LOCAL_HOST_NAME, PORT)
    s.bind((LOCAL_HOST_NAME, int(PORT)))
    s.listen(MAX_NODE_COUNT)
    for i in range(expectedNodeNumber):
        connection, address = s.accept()
        client_data = connection.recv(1024)
        message = receive_server_message(client_data)
        IN_CHANNELS[message["node_id"]] = connection
    global IS_LISTENING_TO_ALL_NODES
    IS_LISTENING_TO_ALL_NODES = True
    while not IS_LISTENING_TO_ALL_NODES or not IS_CONNECTED_TO_ALL_NODES:
        continue
    for nodeId in IN_CHANNELS:
        threading.Thread(target=node_listening_handler, args=(IN_CHANNELS[nodeId], nodeId,)).start()
    if IS_COORDIANTOR:
        while True:
            connection, address = s.accept()
            client_data = connection.recv(1024)
            message = receive_client_message(client_data)
            CLIENT_CHANNELS[message["node_id"]] = connection
            threading.Thread(target=client_handler, args=(connection, message["node_id"],)).start()

def receive_client_message(raw_data):
    message = pickle.loads(raw_data) 
    info = None
    if "content" not in message:
        info = message
    else:
        info = message["content"]
    return info

def client_handler(connection, clientId):
    while True:
        received_time = time.time()
        header_struct = connection.recv(4)
        if len(header_struct) < 1:
            print(received_time, "-", clientId, "disconnected")
            # TotalOrdering.nodeFailed(nodeID)
            CLIENT_CHANNELS.pop(clientId)
            return
        unpack_res = struct.unpack('i',header_struct)
        size = unpack_res[0] 
        recv_size = 0
        total_data = b''
        while recv_size < size:
            recv_data = connection.recv(size - recv_size)
            if len(recv_data) < 1:
                print(received_time, "-", clientId, "disconnected")
                CLIENT_CHANNELS.pop(clientId)
                return 
            recv_size += len(recv_data)
            total_data += recv_data
        message = receive_client_message(total_data)
        if message:
            # TotalOrdering.ReceiveMessage(message)
            Coordinator.receiveClientMessage(message)

def node_listening_handler(connection, nodeID):
    while True:
        received_time = time.time()
        header_struct = connection.recv(4) 
        if len(header_struct) < 1:
            print(received_time, "-", nodeID, "disconnected")
            # TotalOrdering.nodeFailed(nodeID)
            OUT_CHANNELS.pop(nodeID)
            return
        unpack_res = struct.unpack('i',header_struct)
        size = unpack_res[0] 
        recv_size = 0
        total_data = b''
        while recv_size < size:
            recv_data = connection.recv(size - recv_size)
            if len(recv_data) < 1:
                print(received_time, "-", nodeID, "disconnected")
                # TotalOrdering.nodeFailed(nodeID)
                OUT_CHANNELS.pop(nodeID)
                return 
            recv_size += len(recv_data)
            total_data += recv_data
        message = receive_server_message(total_data)
        if message:
            print(message)
            # TotalOrdering.ReceiveMessage(message)
            if IS_COORDIANTOR:
                Coordinator.receiveServerMessage(message)
            else:
                Server.receiveServerMessage(message)
            print("259", message)

if __name__ == "__main__":
    start_node()
