import json
from collections import namedtuple
from types import SimpleNamespace
import time

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


class Client:
    def __init__(self, clientId, sendMessage):
        self.clientId = clientId
        self.sendMessage = sendMessage
        self.isBegin = False
        self.transactionId = None

    def validator(self, userInput):
        if self.isBegin:
            return True
        elif userInput == "BEGIN":
            self.isBegin = True
            self.transactionId = str(time.time()) + self.clientId
            return True

    def userInput(self, userInput):
        userInput = userInput.strip()
        isValid = self.validator(userInput)
        if not isValid:
            return
        print(userInput in ["BEGIN", "COMMIT", "ABORT"])
        if userInput in ["BEGIN", "COMMIT", "ABORT"]:
            print("here")
            message = Message(clientId=self.clientId, action=userInput, transactionId=self.transactionId)
        else:
            userInput = userInput.split(" ")
            userInput[1] = userInput[1].split(".")
            print(47)
            message = Message(clientId=self.clientId, 
                              action=userInput[0],
                              serverId=userInput[1][0],
                              accountId=userInput[1][1],
                              transactionId=self.transactionId)
            if len(userInput) == 3:
                message.amount=int(userInput[2])
        print(message.__dict__)
        self.sendMessage(json.dumps(message.__dict__))
    
    def receiveMessage(self, message):
        message = messageJsonDecod(message)


    # need a function to set isBegin False

        

        

            
    