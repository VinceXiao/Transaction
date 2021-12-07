import json
from types import SimpleNamespace

def messageJsonDecod(messageDict):
    return SimpleNamespace(**messageDict)


class AccountMessage:
    def __init__(self, serverId, accountId, amount, clientId, lock):
        self.serverId = serverId
        self.accountId = accountId
        self.amount = amount
        self.lock = lock
        self.clientId = clientId

class Server:
    def __init__(self, serverId, coordinatorId, sendMessageToServer):
        self.serverId = serverId
        self.sendMessageToServer = sendMessageToServer
        self.accounts = {}
        self.coordinatorId = coordinatorId

    def receiveServerMessage(self, rawMessage):
        message = json.loads(rawMessage, object_hook=messageJsonDecod)
        print("receive Server Message", message.__dict__)
        if message.accountId not in self.accounts:
            self.accounts[message.accountId] = {"amount": 0, "lock": message.lock}
            reply = AccountMessage(self.serverId, message.accountId, 0, message.clientId, message.lock)
        else:
            if self.accounts[message.accountId]["lock"] == "WRITE":
                reply = AccountMessage(self.serverId, message.accountId, None, message.clientId, None)
            else:
                self.accounts[message.accountId]["lock"] == message.lock
                reply = AccountMessage(self.serverId, message.accountId, self.accounts[message.accountId]["amount"], message.clientId, message.lock)
        self.sendMessageToServer(json.dumps(reply.__dict__))
        print(reply.__dict__)
        print(self.accounts)