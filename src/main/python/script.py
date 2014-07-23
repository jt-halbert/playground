#!/usr/bin/env python

from email.parser import Parser
import json
import os

def getFilenames(base):
    l = []
    for root, dirs, files in os.walk(base):
        if len(files) > 0:
            l.extend([os.path.join(root,name) for name in files])
    return l

def convertEmailToJSON(filepointer):
    email = Parser().parse(filepointer)
    return json.dumps({"headers" : dict(email.items()),
        "payload" : email.get_payload(decode=False).decode("utf-8","ignore")})


