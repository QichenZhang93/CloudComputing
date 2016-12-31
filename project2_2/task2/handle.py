from flask import Flask
from flask import request
import os
import os.path
import sys

app = Flask(__name__)

@app.route("/")
def work():
	lang = os.getenv('LANG_INPUT')
	prog = os.getenv('PROG_INPUT')
	progfile = 'prog.py'
	resultfile = 'output'
	sys.stdout = open(progfile, 'w')
	sys.stdout.write(prog)
	os.system('python prog.py > output')
	response = open('output', 'r').read()
	return response

if __name__ == "__main__":
	app.run(debug=True, host='0.0.0.0', port=5000)
