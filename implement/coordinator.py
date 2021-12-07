import json
from collections import namedtuple, deque
import threading

import time


from types import SimpleNamespace

def messageJsonDecod(messageDict):
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
    def __init__(self, serverId, accountId, amount, clientId, lock, transactionId):
        self.serverId = serverId
        self.accountId = accountId
        self.amount = amount
        self.clientId = clientId
        self.lock = lock
        self.transactionId = transactionId


class Transaction:
    def __init__(self, transactionId, clientId):
        self.clientId = clientId
        self.transactionId = transactionId
        self.accounts = {}
        self.locks = {}
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
        print(message.__dict__)
        clientId = message.clientId
        action = message.action
        if clientId not in self.transactions:
            if action == "BEGIN":
                self.transactions[clientId] = Transaction(message.transactionId, clientId)
                threading.Thread(target=self.processTransaction, args=(clientId,)).start()

                # start a thread to process transaction
            else:
                # return value to abort
                pass
        else:
            # while clientId has a processing transaction
            if self.transactions[clientId].transactionId == message.transactionId:
                self.transactions[clientId].operations.append(Operation(message))
            else:
                # while the transaction id is different from the current transaction id
                pass

    def receiveServerMessage(self, rawMessage):
        accountMessage = json.loads(rawMessage, object_hook=messageJsonDecod)
        clientId = accountMessage.clientId
        self.transactions[clientId].reply = accountMessage
        

    def processTransaction(self, clientId):
        transaction = self.transactions[clientId]
        while True:
            if transaction.operations:
                self.checkAccountInfo(transaction.operations[0], clientId, transaction.transactionId)
                while not transaction.reply:
                    time.sleep(0.01)
                accountInfo = transaction.reply
                transaction.reply = None
                if accountInfo.lock:
                    operation = transaction.operations.popleft()
                    abort = self.executeOperation(transaction, operation, accountInfo)
                    if abort:
                        del self.transactions[clientId]
                        return
                    continue # skip waiting
            time.sleep(0.1)
                
    def checkAccountInfo(self, operation, clientId, transactionId):
        accountName = operation.serverId + "." + operation.accountId
        if accountName in self.transactions[clientId].locks and self.transactions[clientId].locks[accountName] == "WRITE":
            print("enter here")
            replyMessage = AccountMessage(operation.serverId, operation.accountId, self.transactions[clientId].accounts[accountName], clientId, "WRITE", transactionId)
            self.transactions[clientId].reply = replyMessage
        else:
            acquireMessage = AccountMessage(operation.serverId, operation.accountId, operation.amount, clientId, None, transactionId)
            if operation.action == "BALANCE":
                acquireMessage.lock = "READ"
            else:
                acquireMessage.lock = "WRITE"
            self.sendMessageToServer(json.dumps(acquireMessage.__dict__), operation.serverId)

    def executeOperation(self, transaction, operation, accountInfo):
        accountName = accountInfo.serverId + "." + accountInfo.accountId
        transaction.locks[accountName] = accountInfo.lock
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
        print(transaction.accounts)
        


                    

    