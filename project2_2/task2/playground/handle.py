import os
import os.path
import sys
import subprocess

def work():
	lang = os.getenv('LANG_INPUT')
	prog = os.getenv('PROG_INPUT')
	fw = open('/home/prog', 'w')
	fw.write(prog)
	print subprocess.check_output('python /home/prog')
work()
