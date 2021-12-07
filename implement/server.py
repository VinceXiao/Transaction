import json
from types import SimpleNamespace

def messageJsonDecod(messageDict):
    return SimpleNamespace(**messageDict)


class AccountMessage:
    def __init__(self, serverId, accountId, amount, clientId, lock, transactionId):
        self.serverId = serverId
        self.accountId = accountId
        self.amount = amount
        self.clientId = clientId
        self.lock = lock
        self.transactionId = transactionId

class Server:
    def __init__(self, serverId, coordinatorId, sendMessageToServer):
        # accounts: {account_id: 
        #               amount: int,
        #               has_wlock: boolean,
        #               locks: [transaction_id]
        #           }
        self.serverId = serverId
        self.sendMessageToServer = sendMessageToServer
        self.accounts = {}
        self.coordinatorId = coordinatorId

    def receiveServerMessage(self, rawMessage):
        message = json.loads(rawMessage, object_hook=messageJsonDecod)
        print("receive Server Message", message.__dict__)
        has_wlock = True if message.lock == "WRITE" else False
        if message.accountId not in self.accounts: # new account
            self.accounts[message.accountId] = {"amount": 0, "has_wlock": has_wlock, "locks": [message.transactioonId]}
            reply = AccountMessage(self.serverId, message.accountId, 0, message.clientId, message.lock, message.transactionId)
        else:
            if self.accounts[message.accountId]["has_wlock"] == True: # try to read/write account that has wlock, reject
                reply = AccountMessage(self.serverId, message.accountId, None, message.clientId, None, message.transactionId)
            else:
                if message.lock == "WRITE":
                    # check if it's promotion request
                    if len(self.accounts["locks"]) == 1 and message.transactionId == self.accounts["locks"][0]:
                        lock = message.lock
                        self.accounts[message.accountId]["has_wlock"] = True
                    else:
                        lock = None
                    reply = AccountMessage(self.serverId, message.accountId, None, message.clientId, lock, message.transactionId)
                else:
                    if message.transactionId not in self.accounts[message.accountId]["locks"]:
                        self.accounts[message.accountId]["locks"].append(message.transactionId)
                    reply = AccountMessage(self.serverId, message.accountId, self.accounts[message.accountId]["amount"], message.clientId, message.lock)
        self.sendMessageToServer(json.dumps(reply.__dict__))
        print(reply.__dict__)
        print(self.accounts)