#!/usr/bin/env python
import dropbox_exploit
import core
import cmd
import sys

class deepack_cmds(cmd.Cmd):
	def do_get(self, args):
		return False

	def do_put(self, args):
		return False

	def do_list(self, args):
		return False

class dropbox_cmds(deepack_cmds):
	def __init__(self, **kwargs):
		self.wrapper = dropbox_exploit.wrapper(**kwargs)

	def do_put(self, filename):
		key = self.wrapper.put(filename)
		print (filename, key)

	def do_get(self, internal_handle, output_name = None):
		print (self.wrapper.get(internal_handle, output_name))

	def do_list(self, args=''):
		for line in self.wrapper.list_files(args):
			print (line)

	def do_update(self, args):
		pass # TODO: deal with file expiry



if __name__ == '__main__':
	if len(sys.argv) == 1:
		
		# interactive mode 

		con = deepack_cmds()
		con.cmdloop()

	elif len(sys.argv) <= 2:

		# usage

		print ("usage: python2 %s <type>-<instance> <command> [args [...]]" % sys.argv[0])
		exit(-1)

	else:
		
		# direct input format: <type>-<instance> command [args [...]]

		db_hook = core.db_wrapper()

		module,instance = sys.argv[1].split('-')
		command = sys.argv[2]
	
		if (instance == '*'):
			# TODO: some special case to handle all of a class
			pass
	
		service = db_hook.get_service(module, instance)	
		if (service == []):
			print ("'%s-%s' is not a recognized service." % (module, instance))
			exit(-1)

		if module == 'dropbox':
			cmd_hook = dropbox_cmds(instance_name=instance, APP_KEY = 'fhf5x0mwmzke0pn', APP_SECRET = '8tkuc464y3m3t5o', chunk_size = core.gaussian(512 * 1024))
		else:
			print ("the '%s' module is currently unsupported through this interface" % (module))
			exit(-1)

		
		if command == 'put':
			cmd_hook.do_put(sys.argv[3])
		elif command == 'get':
			if (sys.argv[4:] == []):
				filename = None
			else:
				filename = sys.argv[4]

			cmd_hook.do_get(sys.argv[3], filename)
		elif command == 'list':
			if (sys.argv[3:] == []):
				complete = ''
			else:
				complete = sys.argv[3]
			cmd_hook.do_list(complete)
		elif command == 'update':
			pass

