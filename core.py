#!/usr/bin/env python2

import sqlite3
import os, os.path, shutil
import tempfile
from hashlib import sha1

# Utility Functions
def git_style_hash(filename):
	cksum = sha1("blob " + str(os.path.getsize(filename)) + "\0")
	f = open(filename, 'rb')
	try:
		cksum.update(f.read())
	finally:
		f.close()

	return cksum.hexdigest()

# size distribution generators
def constant(size):
	while True:
		yield size

import random
def gaussian(mu, sigma=None):
	if sigma == None:
		sigma = 0.05 * mu

	while True:
		yield int(random.gauss(mu, sigma))

		


# Database Wrapper
class db_wrapper:
	
	def __init__(self):
		self.handle = self.init_db()

	def init_db(self, database='storage.db', force=False):
		to_init = (not os.path.exists(database)) or force
		handle = sqlite3.connect(database)

		if to_init:
			cursor = handle.cursor()
			cursor.execute("CREATE TABLE access_tokens( \
								service				TEXT NOT NULL, \
								instance_name		TEXT NOT NULL, \
								key					TEXT NOT NULL, \
								secret				TEXT NOT NULL, \
								UNIQUE (service, instance_name) \
							)")
			cursor.execute("CREATE TABLE files( \
								path				TEXT NOT NULL, \
								date				DATE NOT NULL, \
								size				INTEGER NOT NULL, \
								checksum			TEXT NOT NULL, \
								service				TEXT NOT NULL, \
								instance_name		TEXT NOT NULL, \
								internal_handle		TEXT NOT NULL, \
								UNIQUE (internal_handle), \
								FOREIGN KEY(service, instance_name) REFERENCES access_tokens(service, instance_name)\
							)")
			cursor.execute("CREATE TABLE fragments( \
								internal_handle		TEXT NOT NULL, \
								create_date			DATE NOT NULL, \
								access_date			DATE NOT NULL, \
								fragment_index		TEXT NOT NULL, \
								size				INTEGER NOT NULL, \
								UNIQUE(fragment_index, internal_handle), \
								FOREIGN KEY(internal_handle) REFERENCES files(internal_handle) \
							)") 
			handle.commit()

		return handle


	# Soft functions (for tab-complete)
	def get_service(self, service_name='', instance_name=''):
		cursor = self.handle
		return cursor.execute("SELECT service, instance_name FROM access_tokens WHERE service LIKE ? AND instance_name LIKE ?", (service_name+'%', instance_name+'%')).fetchall()
	

	def get_files(self, service, instance_name, path='', internal_handle=''):
		cursor = self.handle
		return cursor.execute("SELECT service, instance_name,  date, path, internal_handle FROM files WHERE service = ? AND instance_name = ? AND path LIKE ? AND internal_handle LIKE ? ", (service, instance_name, path+'%', internal_handle+'%')).fetchall()

	# Hard functions
	def exists(self, internal_handle):
		cursor = self.handle
		return cursor.execute("SELECT size, checksum, date, path FROM files WHERE internal_handle = ? LIMIT 1", [internal_handle]).fetchone() 
	
	def get_access_token(self, service, name):
		cursor = self.handle
		return cursor.execute("SELECT key, secret FROM access_tokens WHERE service = ? AND instance_name = ? LIMIT 1" , (service, name)).fetchone()
	
	def store_oauth_token(self, service, name, key, secret):
		cursor = self.handle
		cursor.execute("INSERT INTO access_tokens VALUES (?, ?, ?, ?)", (service, name, key, secret))
		cursor.commit()
	

	def get_file_fragments(self, internal_handle):
		cursor = self.handle
		return cursor.execute("SELECT fragment_index, size FROM fragments WHERE internal_handle = ?", [internal_handle]).fetchall()


# CORE FRAMEWORK
class framework:
	def __init__(self, defaults, **kwargs):
		self.chunk_size		= constant(4 * (1024 ** 2)) # 4 Mb as a default chunk size
		self.version		= '0.2b'
		self.author			= 'Zak Blacher'
		self.service		= None # 'Abstract Class'
		self.instance_name			= None # 'Abstract Class' <- instances have names
		self.__dict__.update(defaults, **kwargs)
		
		self.db_hook		= db_wrapper()


	# functions required to be overwritten:
	def get(self, fileName):
		pass

	def put(self, fileName):
		pass

	def list(self, fileName=None):
		pass

	def fragment_generator(self, internal_handle):
		pass

	def auth(self):
		pass

	# these should be okay
	def fragment(self,filename, **kwargs):
		tmpdir = tempfile.mkdtemp()
		#TODO: Progress Bar?
		cur_chunk = 0 
		with open(filename, 'rb') as file_handle:
			while True:
				data = file_handle.read(self.chunk_size.next())
				print ("chunk %3d: size: %d" % (cur_chunk, len(data)))
				cur_chunk += 1
				chunk_name = os.path.join(tmpdir, filename)
				tmpfile = open(chunk_name, 'wb')
				tmpfile.write(data)
				tmpfile.close()

				yield chunk_name
				if (len(data) == 0): break			

		shutil.rmtree(tmpdir)

	def reassemble(self, internal_handle, output_file=None):

		
		results = self.db_hook.exists(internal_handle)
			
		if (results == None):
			print ("Error. '%s' not recognized as an internal_handle" % internal_handle)
			return

		(size,checksum,date,path) = results

		tmpdir = tempfile.mkdtemp()
		tmpfile = os.path.join(tmpdir, internal_handle+".incomplete")

		file_handle = open(tmpfile, 'wb')
		for fragment in self.reassemble_generator(internal_handle):
			while True:
				try:
					file_handle.write(fragment.read())
					break
				except:
					print ("FAILED on Fragment '%s', retrying" % fragment)

		file_handle.close()

		if (output_file == None):
			output_file = os.path.join(path+"-"+checksum+"-"+str(date))

		if (size == os.path.getsize(tmpfile) and checksum == git_style_hash(tmpfile)):
			print ("Data matches!")
			shutil.copyfile(tmpfile, output_file)
		else:
			print (tmpfile)
			print ("Error recovering file.")

		shutil.rmtree(tmpdir)


	def register_file(self, fileName, hook_name, responses, response_format='json'):
		cursor = self.db_hook.handle.cursor()

		checksum = git_style_hash(fileName)
		internal_handle = hook_name #TODO: use something else as the internal handle instead?

		try:
			cursor.execute("INSERT INTO files VALUES (?, ?, ?, ?, ?, ?, ?)", (fileName, os.path.getatime(fileName), os.path.getsize(fileName), checksum, self.service, self.instance_name, internal_handle))

			for line in responses:
				cursor.execute("INSERT INTO fragments VALUES (?, ?, ?, ?, ?)", (internal_handle, line['client_mtime'], line['modified'], line['rev'], line['size']))
			self.db_hook.handle.commit()

		except sqlite3.IntegrityError:
			print ("This file already exists in the database. Aborting registration.")
			self.db_hook.handle.rollback()

