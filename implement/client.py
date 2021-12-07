import json
from collections import namedtuple
from types import SimpleNamespace

def messageJsonDecod(messageDict):
    # return namedtuple('Message', messageDict.keys())(*messageDict.values())
    return SimpleNamespace(**messageDict)

class Message:
    def __init__(self, clientId=None, action=None, serverId=None, accountId=None, amount=None):
        self.clientId = clientId
        self.action = action
        self.serverId = serverId
        self.accountId = accountId
        self.amount = amount



class Client:
    def __init__(self, clientId, sendMessage):
        self.clientId = clientId
        self.sendMessage = sendMessage
        self.isBegin = False

    def validator(self, input):
        if self.isBegin:
            return True
        elif input == "BEGIN":
            self.isBegin = True
            return True

    def userInput(self, userInput):
        userInput = userInput.strip()
        if userInput in ["BEGIN", "COMMIT", "ABORT"]:
            message = Message(clientId=self.clientId, action=userInput)
        else:
            userInput = userInput.split(" ")
            userInput[1] = userInput[1].split(".")
            message = Message(clientId=self.clientId, 
                              action=userInput[0],
                              serverId=userInput[1][0],
                              accountId=userInput[1][1],
                              amount=userInput[2])
        self.sendMessage(json.dumps(message.__dict__))
    
    def receiveMessage(self, message):
        message = messageJsonDecod(message)


    # need a function to set isBegin False

        

        

            
    