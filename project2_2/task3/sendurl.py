import requests

prog = '''
import sys
sys.stdout.write('Hello')
sys.stderr.write('err')
'''

r = requests.post("http://qichenzccelb-1406576089.us-east-1.elb.amazonaws.com:80", data={'lang' : 'python', 'prog': prog})
print r.text
