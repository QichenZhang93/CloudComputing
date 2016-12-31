#from docker import Client
#cli=Client(base_url='unix://var/run/docker.sock')
#response = [line for line in cli.build(path='./playground/', dockerfile='./Dockerfile', tag='qichenz', rm=True)]
#print response
import subprocess
subprocess.check_output('docker build -t qichenz ./playground/')
