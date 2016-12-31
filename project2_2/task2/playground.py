from flask import Flask, request
from docker import Client
from time import sleep
import socket

app = Flask(__name__, static_folder='site', static_url_path='')

@app.route("/", methods=['GET', 'POST'])
def handle():
    if request.method == 'POST':
        lang = request.form['lang']
        prog = request.form['prog']

        # Implementation goes here.
        #
        # 1) Launch a container and run prog inside it
        # 2) Capture the output and return them as the response.
        #
        # Both stdout and stderr should be captured.

        ### BEGIN STUDENT CODE ###
	# write prog into ./playground/progfile/
        print '-----language:', lang
        print '-----program:', prog
        print '--------------------------------------------'
        fw = open('./playground/progfile/prog', 'w')
        fw.write(prog)
        fw.close()
        command = lang
        # run with shared volume
        cmd_container = lang + ' ' + '/home/prog'
        #print 'cmd:', cmd_container
        cli = Client(base_url='unix://var/run/docker.sock')
        container = cli.create_container(
            image='qichenz:test',
            command=cmd_container,
            volumes=['/home'],
            host_config=cli.create_host_config(binds={'/home/ubuntu/task2/playground/progfile': {'bind': '/home','mode': 'rw'}})
        )
        response = cli.start(container=container.get('Id'))
        sleep(1)
        response = cli.logs(container.get('Id'), stdout=True, stderr=False)
        #print 'stdout:', response
        response = response + cli.logs(container.get('Id'), stderr=True, stdout=False)
        #print 'stderr:', cli.logs(container.get('Id'), stderr=True)
        print response
        ### END STUDENT CODE ###
        return response

    else:
        return app.send_static_file("index.html")

if __name__ == '__main__':
    app.run(debug=False, host="0.0.0.0", port=5000)
