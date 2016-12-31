from flask import Flask, request, redirect
from docker import Client

import requests
import socket

app = Flask(__name__, static_folder='site', static_url_path='')

@app.route("/", methods=['GET', 'POST'])
def handle():
    if request.method == 'POST':
        lang = request.form['lang']
        prog = request.form['prog']

        # Implementation goes here.
        #
        # Convert request, if necessary, and forward it
        # to the cluster pool
        # Copy the response back to the client

        ### BEGIN STUDENT CODE ###
        print lang
        print prog
        r = requests.post('http://qichenzccelb-1174366880.us-east-1.elb.amazonaws.com:80', data={'lang':lang, 'prog':prog})
        ### END STUDENT CODE ###
        print r.text
        return r.text
        
    else:
        return app.send_static_file("index.html")

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5000)
