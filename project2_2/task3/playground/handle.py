from flask import Flask, request
import socket
import subprocess

app = Flask(__name__)

@app.route("/", methods=['GET', 'POST'])
def hello():
	if request.method == 'POST':
		print 'receive post'
		lang = request.form['lang']
		prog = request.form['prog']
		fw = open('./prog', 'w')
		fw.write(prog)
		fw.close()
		command = lang + ' ' + './prog'
		print command
		process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
		print process
		out, err = process.communicate()
		print out, err
		response = ''
		if len(out) != 0:
			response = response + out
			print response
		if len(err) != 0:
			if len(response) != 0:
				response = response
			response = response + err
			print response
		return response
	else:
		return "Hello"

if __name__ == '__main__':
	app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)
