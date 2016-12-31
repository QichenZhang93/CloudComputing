from docker import Client
from time import sleep
cli = Client(base_url='unix://var/run/docker.sock')
container = cli.create_container(
    image='qichenz:test',
    command='ruby /home/prog',
    volumes=['/home'],
    host_config=cli.create_host_config(binds={'/home/ubuntu/task2/playground/progfile': {'bind': '/home','mode': 'rw'}})
)
print(container)
response = cli.start(container=container.get('Id'))
#print response
sleep(1)
print cli.logs(container.get('Id'), stdout=True)
print cli.logs(container.get('Id'), stderr=True)
