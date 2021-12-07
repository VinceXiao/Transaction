import json
from collections import namedtuple, deque
import threading

import time

# def messageJsonDecod(messageDict):
# 	return namedtuple('Message', messageDict.keys())(*messageDict.values())
from types import SimpleNamespace

def messageJsonDecod(messageDict):
    # return namedtuple('Message', messageDict.keys())(*messageDict.values())
    return SimpleNamespace(**messageDict)


class Message:
    def __init__(self, clientId=None, action=None, serverId=None, accountId=None, amount=None, transactionId=None):
        self.clientId = clientId
        self.action = action
        self.serverId = serverId
        self.accountId = accountId
        self.amount = amount
        self.transactionId = transactionId

class AccountMessage:
    def __init__(self, serverId, accountId, amount, clientId, action):
        self.serverId = serverId
        self.accountId = accountId
        self.amount = amount
        self.action = action
        self.clientId = clientId


class Transaction:
    def __init__(self, transactionId, clientId):
        self.clientId = clientId
        self.transactionId = transactionId
        self.accounts = {}
        self.operations = deque([])
        self.reply = None

class Operation:
    def __init__(self, message):
        self.action = message.action
        self.serverId = message.serverId
        self.accountId = message.accountId
        self.amount = message.amount


class Coordinator:
    def __init__(self, serverId, serverNames, sendMessageToServer, sendMessageToClient):
        self.serverId = serverId
        self.serverNames = serverNames
        self.transactions = {}
        self.sendMessageToServer = sendMessageToServer
        self.sendMessageToClient = sendMessageToClient

    def receiveClientMessage(self, rawMessage):
        message = json.loads(rawMessage, object_hook=messageJsonDecod)
       
       
        # clientId = message.clientId
        # action = message.action
        # if clientId not in self.transactions:
        #     if action == "BEGIN":
        #         self.transactions[clientId] = Transaction(message.transactionId, clientId)
        #         threading.Thread(target=self.processTransaction, args=(clientId,)).start()

        #         # start a thread to process transaction
        #     else:
        #         # return value to abort
        #         pass
        # else:
        #     # while clientId has a processing transaction
        #     if self.transactions[clientId].transactionId == message.transactionId:
        #         self.transactions[clientId].operations.append(Operation(message))
        #     else:
        #         # while the transaction id is different from the current transaction id
        #         pass

    def receiveServerMessage(self, rawMessage):
        accountMessage = messageJsonDecod(rawMessage)
        clientId = accountMessage.clientId
        self.transactions[clientId].reply = accountMessage
        

    def processTransaction(self, clientId):
        transaction = self.transactions[clientId]
        while True:
            if transaction.operations:
                accountInfo = self.checkAccountInfo(transaction.operations[0], clientId)
                while not transaction.reply:
                    time.sleep(0.01)
                accountInfo = transaction.reply
                transaction.reply = None
                if accountInfo.getLock:
                    operation = transaction.operations.popleft()
                    abort = self.executeOperation(transaction, operation, accountInfo)
                    if abort:
                        del self.transactions[clientId]
                        return
            else:
                time.sleep(0.1)
                
    def checkAccountInfo(self, operation, clientId):
        acquireMessage = AccountMessage(operation.serverId, operation.accountId, None, operation.action, clientId)
        self.sendMessageToServer(json.dumps(acquireMessage.__dict__), operation.serverId)

    def executeOperation(self, transaction, operation, accountInfo):
        accountName = accountInfo.serverId + "." + accountInfo.accountId
        abort = False
        if operation.action == "DEPOSIT":
            transaction.accounts[accountName] = transaction.accounts.get(accountName, accountInfo.amount) + operation.amount
            print(accountName, transaction.accounts[accountName])
            pass
        elif operation.action == "BALANCE":
            print(transaction.accounts)
            print(accountInfo.amount)
            pass
        elif operation.action == "WITHDRAW":
            transaction.accounts[accountName] = transaction.accounts.get(accountName, accountInfo.amount) + operation.amount
            pass
        elif operation.action == "COMMIT":
            pass
        elif operation.action == "ABORT":
            abort = True
        


                    

    