import json

f = open('person.json')
data = json.load(f)
    
print(data)