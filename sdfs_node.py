#!/usr/bin/python3
import os
import sys
import time
import shutil
import json
import random
import socket
import tqdm
import random
import threading
import queue
from random import shuffle
from failure_detector import RingFailureDetector

'''
#
# message json field definition
# type: message type, present in all messages, can be the following value (string)
#		ret_file_ls	: Message for requesting retrieve file list (including version)
#		put_file	: Message for requesting put file
#		get_file	: Message for requesting retrieve certain file
#		del_file	: Message for requesting delete certain file
#		get_master	: Message for requesting retrieve current master ID on the node
#		halt		: Message for halting slave
#		ack			: Message for responding to requests
#		election	: Message for electing new master (does not have ack back)
#		elected		: Message for notifying end of election (does not have ack back)
# 
# filename: target file w.r.t. this request, present in put_file, get_file, del_file,
# 			value is sdfsfilename for the target file
#
# size: target file size w.r.t. this request, present in put_file,
# 			value is the target file size
#
# version: target file version w.r.t this request, present in put_file,
#			value is the target file version
#
# response_code: response code w.r.t. to the request, "always" present in ack
#			value is 0 for success and 1 for failure
#
# response: answer to the requests, present in ack
#			value dependent on corresponding requests:
#			ret_file_ls	: A json storing filenames and version numbers [a list of dictionary having filename and version] 
# 			put_file	: No such field in ack
# 			get_file	: file size for the requested file
# 			del_file	: No such field in ack
#			get_master	: ID for sdfs master
# 			halt		: No such field in ack
#			
# candidate: target election node w.r.t. this election, present in election and elected,
# 			value is current highest ID for sdfs node
#
# initiator: node that started the election, present in election and elected, 
# 			value is ID for initiating sdfs node
#
'''

class sdfsfilesystem:
	def __init__(self, master_port):
		self.start_fd_wait = 3
		self.master_store_dir = 'master'
		self.master_dead, self.master_run, self.new_master = 0, 0, 0
		self.file_list = {}														 #{'filename':filename, 'vm':list of ip addresses}
		self.w_dup_num = 4
		self.client_listen_port = master_port
		self.membership_list = []
		self.vm_list = ["fa19-cs425-g43-01.cs.illinois.edu", "fa19-cs425-g43-02.cs.illinois.edu",
						"fa19-cs425-g43-03.cs.illinois.edu", "fa19-cs425-g43-04.cs.illinois.edu",
						"fa19-cs425-g43-05.cs.illinois.edu", \
						"fa19-cs425-g43-06.cs.illinois.edu", "fa19-cs425-g43-07.cs.illinois.edu",
						"fa19-cs425-g43-08.cs.illinois.edu", "fa19-cs425-g43-09.cs.illinois.edu",
						"fa19-cs425-g43-10.cs.illinois.edu"]

	def master_loop(self, newly_elected=0):
		'''
		SDFS slave main thread
		Serve commands from master node and clients indefinitely
		Start election process if master node detected to be failed by failure detection component
		TODO
		- should add functionality that when in election, prevent election message from lower initiator from traversing ring
		:return:
		'''
		if self.new_master == 1:
			self.get_file_list_from_all_vms()

		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		print("PORTTTTTT {}".format(self.client_listen_port))
		s.bind(("0.0.0.0", self.client_listen_port))
		s.listen(5)
		# self.master = self.fd.self_node.id
		#self.master_dead = 0
		#self.master_run = 1
		print("Enter Master Loop")
		while self.master_run:
			s.settimeout(0.5)
			try:
				conn, _ = s.accept()
				print("master accepted connection")
				try:
					data = self.recv_json_from_sock(conn)
				except ConnectionResetError:
					print('Client connection error')
					conn.close()
					continue
				if not data:
					print('Closing connection')
					conn.close()
					continue
				print('Got message from client {0}'.format(data))

				msg = self.handle_request(data, conn)

				print('sending msg {0}'.format(msg))
				if msg == 1:
					# halt
					conn.close()
					break
				conn.sendall(msg)
				conn.close()
				f_list = self.failure_check()
				if f_list:
					self.redistribution(f_list)

			except socket.timeout:
				f_list = self.failure_check()
				self.redistribution(f_list)
				continue
		s.close()
		self.master_dead = 1

	def failure_check(self):
		mem_list = self.fd.ret_membership_list()
		failure = []
		for ip in self.membership_list:
			if ip not in mem_list:
				failure.append(ip)
				print('Failure Detected in Master: IP {0}',format(ip))
		self.membership_list = mem_list
		return failure

	def handle_request(self, packet, conn):
		with open("master_log.log", "a") as fp:
			data = json.loads(packet)
			if data['type'] == 'ret_file_ls':
				print('Handling ls')
				fp.write("{}\tHandling ls; msg content: {}\n".format(time.time(), packet))
				msg = self.handle_ls(data)
			elif data['type'] == 'put_file':
				print('put file')
				fp.write("{}\tput file; msg content: {}\n".format(time.time(), packet))
				msg = self.master_put_file(data, conn)
			elif data['type'] == 'get_file':
				print('get file')
				fp.write("{}\tget file; msg content: {}\n".format(time.time(), packet))
				msg = self.master_get_file(data, conn)
			elif data['type'] == 'del_file':
				print('del file')
				fp.write("{}\tdel file; msg content: {}\n".format(time.time(), packet))
				msg = self.master_del_file(data, conn)
			elif data['type'] == 'halt':
				print('Handling halt request')
				return 1
			elif data['type'] == 'get_master':
				print('Handling get master')
				msg = json.dumps({'type': 'ack', 'response_code': 0, 'response': self.master}).encode()
			elif data['type'] == 'get_file_list':
				print('Handling get file list')
				msg = self.master_get_file_ls(data, conn)
			else:
				print('Wrong argument passed in')
				msg = None
		return msg

	def redistribution(self, failures):
		mem_list = self.fd.ret_membership_list()
		if not self.file_list: return		#no file exist.
		for ip in failures:
			for f_name, dict in self.file_list.items():  # for every file, delete file of failed vm
				if ip in dict['vm']:
					dict['vm'].remove(ip)

		#replicate until every file has 4 replicas
		for f_name, dict in self.file_list.items():
			shuffle(mem_list)
			n = len(mem_list)
			idx = 0
			while len(dict['vm'])  < 4 and idx < n:
				if mem_list[idx] not in dict['vm']:
					self.send_replicate_request(mem_list[idx], dict['vm'], f_name)
					print('LENGTH of file list {0} is {1}.  Length of mem list is {2}'.format(f_name, len(dict['vm']), n))
					print("Redistributing file {0} to {1}".format(f_name, mem_list[idx]))
					dict['vm'].append(mem_list[idx])
				idx += 1
		return

	def get_file_list_from_all_vms(self):
		mem_list = self.fd.ret_membership_list()
		for ip in mem_list:
			while self.master_run:
				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.settimeout(3)
				server_address = (ip, self.slave_listen_port)
				try:
					sock.connect(server_address)
				except:
					print('Connection failure for vm {0}'.format(ip))
					sock.close()
					continue
				try:
					print("Querying vm {0} for version".format(ip))
					msg = {'type': 'ret_file_ls'}
					sock.sendall(json.dumps(msg).encode())
					data = self.recv_json_from_sock(sock)  # TODO get timeout, confirm ack
					data = json.loads(data)
					if not data:
						print('No data received from vm {0}'.format(ip))
						sock.close()
						time.sleep(0.1)
						continue
					elif data['response_code'] == 1:
						print('Got ls response code 1 for vm {0}'.format(ip))
						sock.close()
						time.sleep(0.1)
						continue
					else:
						# CHECK FOR VM THAT HAS MOST RECENT VERSION OF FILE
						print('****GOT FILE LIST FROM {0}****'.format(ip))
						for file in data['response']:
							f_name = file['filename']
							if f_name not in self.file_list.keys():
								self.file_list[f_name] = {'filename': f_name, 'vm': [ip], 'version': file['version'],
														  'time': time.time() - 100}
							else:
								self.file_list[f_name]['vm'].append(ip)
						sock.close()
						break
				except Exception as e:
					print(e)
					sock.close()
					continue

	def send_replicate_request(self, ip_, replicate_list, f_name):
		# add new ip to the filelist
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.settimeout(50)
		for ip_r in replicate_list:
			try:
				request = {"type": "get_replicate", 'filename': f_name, 'ip': ip_r}
				sock.connect((ip_, self.slave_listen_port))
				sock.sendall(json.dumps(request).encode())  # timeout applies, but should not do harm
				data = self.recv_json_from_sock(sock)
				data = json.loads(data)
				if data['response_code'] == 0:
					return
				print(data)
				sock.close()
			except:
				continue
		return

	def handle_ls(self, data):
		'''
		- get the vm list that stores the given file name
		:param filename: filename to be searched
		:return: json message that contains vm lists that stores the given file name
		'''
		f_name = data['filename']
		if f_name in self.file_list.keys():
			filename = data['filename']
			ack = {"type": "ack"}
			ack["response_code"] = 0
			ack["response"] = {"filename": filename, "vm": self.file_list[filename]['vm']}
			msg = json.dumps(ack).encode()
			return msg
		else:
			ack = {"type": "ack"}
			ack["response_code"] = 1
			msg = json.dumps(ack).encode()
			return msg

	def master_put_file(self, data, conn):
		'''
		- receives file from the client and saves in the distributed file system
		:param data: meta data of the file to be received
		:param conn: connection to the client
		:return:
		'''
		f_name, f_size, f_path = data['filename'], data['size'], os.path.join(self.master_store_dir, data['filename'])
		ack, ip_list = {"type": "ack"}, []

		#check if file has been stored within 60 seconds
		if f_name in self.file_list:
			if int(time.time() - self.file_list[f_name]['time']) < 60:   #prompt the client whether to store the file
				recv_ack = {'type':1}
				conn.sendall(json.dumps(recv_ack).encode())
				data = self.recv_json_from_sock(conn)
				try:
					data = json.loads(data)
				except Exception as e:
					print(e)
					exit(1)
				if data['type'] == 0:
					ack["response_code"] = 1
					return json.dumps(ack).encode()
			else:
				recv_ack = {'type': 0}
				conn.sendall(json.dumps(recv_ack).encode())  # send ack to receive file
		else:
			recv_ack = {'type': 0}
			conn.sendall(json.dumps(recv_ack).encode())  # send ack to receive file

		# receive file from the client
		if self.recv_file(f_name, f_size, f_path, conn):
			print('File {0} size {1} received from client successfully'.format(f_name, f_size))
		else:
			print('Did not receive complete file')
			ack["response_code"] = 1
			return json.dumps(ack).encode()

		# get version of the file and idx of the vms to send the file
		if f_name in self.file_list:
			version = self.file_list[f_name]['version'] + 1
			mem_list = self.file_list[f_name]['vm']
		else:
			mem_list = self.fd.ret_membership_list()
			shuffle(mem_list)
			version = 0

		# send file to each vm
		while len(ip_list) < 4:
			if not mem_list:
				print('System has less than 4 active servers. Error')
				break
			ip = mem_list.pop()
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			server_address = (ip, self.slave_listen_port)
			try:
				sock.connect(server_address)  # TODO: add timeout
			except:
				print('Connection failure for vm {0}'.format(ip))
				continue
			try:
				print("Sending data to vm {0}".format(ip))
				msg = {'type': 'put_file', 'filename': f_name, 'size': f_size, 'version': version}
				sock.sendall(json.dumps(msg).encode())
				self.send_file(f_name, f_path, sock)
				data = self.recv_json_from_sock(sock)  # TODO get timeout, confirm ack
				data = json.loads(data)
			finally:
				sock.close()
			if not data:
				print('Sending failure for vm {0}'.format(ip))
			elif data['response_code'] == 1:
				print('Sending failure for vm {0}'.format(ip))
			else:
				ip_list.append(ip)

		# store file in file list
		self.file_list[f_name] = {'filename': f_name, 'vm': ip_list, 'version': version, 'time': time.time()}   #TODO:cur tim
		ack['response_code'] = 0
		return json.dumps(ack).encode()

	def master_get_file(self, data, conn):
		'''
		- receives file from the client and saves in the distributed file system
		:param data: meta data of the file to be received
		:param conn: connection to the client
		:return:
		'''
		f_name = data['filename']
		ack = {"type": "ack"}
		max_version, query_ip = -1, 0

		# get version of the file and idx of the vms to ask for the version
		if f_name in self.file_list:
			version = self.file_list[f_name]['version']
			ip_list = self.file_list[f_name]['vm'].copy()
			shuffle(ip_list)
			ip_list.pop()			   #QUORUM, CHECK 3 REPLICATE SERVERS
		else:
			print('File {0} does not exist.'.format(f_name))
			ack["response_code"] = 1
			ack["response"] = 'File does not exist.'
			return json.dumps(ack).encode()
		print('index of the vm to be queried {0}'.format(ip_list))
		print('version {0}'.format(version))

		# ret_ls_file for each vm and get most recent version
		for ip in ip_list:
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			server_address = (ip, self.slave_listen_port)
			try:
				sock.connect(server_address)
			except:
				print('Connection failure for vm {0}'.format(ip))
				continue
			try:
				print("Querying vm {0} for version".format(ip))
				msg = {'type': 'ret_file_ls'}
				sock.sendall(json.dumps(msg).encode())
				data = self.recv_json_from_sock(sock)  # TODO get timeout, confirm ack
				data = json.loads(data)
				if not data: continue
			finally:
				sock.close()
			if not data:
				print('Sending failure for vm {0}'.format(ip))
				continue
			elif data['response_code'] == 1:
				print('Sending failure for vm {0}'.format(ip))
				continue
			#CHECK FOR VM THAT HAS MOST RECENT VERSION OF FILE
			for file in data['response']:
				if file['filename'] == f_name:
					if file['version'] > max_version:
						max_version = file['version']
						query_ip = ip

		# if query success, return the ip of the slave server with recent version
		if query_ip == 0:
			print('Query failed')
			ack['response_code'] = 1
			return json.dumps(ack).encode()
		print('IP address of the slave to be queried {0}'.format(query_ip))
		ack['response_code'] = 0
		ack['response'] = query_ip
		return json.dumps(ack).encode()

	def master_del_file(self, data, conn):
		f_name = data['filename']
		ack, fail = {"type": "ack"}, 0
		if f_name in self.file_list:
			ip_list = self.file_list[f_name]['vm']
		else:
			ack['response_code'] = 1
			ack['response'] = 'File does not exist'
			return json.dumps(ack).encode()
		# send delete request to every ip that stores the file
		for ip in ip_list:
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			server_address = (ip, self.slave_listen_port)
			try:
				sock.connect(server_address)
			except:
				print('Connection failure with vm {0}'.format(ip))
				fail = 1
			try:
				print("Sending request to delete file {0}  in vm {1}".format(f_name, ip))
				msg = {'type': 'del_file', 'filename': f_name}
				sock.sendall(json.dumps(msg).encode())
				data = self.recv_json_from_sock(sock)  # TODO get timeout, confirm ack
				data = json.loads(data)
			# print('got data {0}'.format(data))
			finally:
				sock.close()
			if not data:
				fail = 1
				print('Sending failure for vm {0}'.format(ip))
			elif data['response_code'] == 1:
				fail = 1
				print('Sending failure for vm {0}'.format(ip))

		# send ack
		if fail:
			ack['response_code'] = 1
		else:
			ack['response_code'] = 0
			del self.file_list[f_name]
		return json.dumps(ack).encode()

	def master_get_file_ls(self, data, conn):
		ack = {"type": "ack"}
		ack['response_code'] = 0
		ack['response'] = [filename for filename in self.file_list]
		return json.dumps(ack).encode()

	def recv_file(self, f_name, file_size, f_path, conn):
		with open(f_path, "wb") as fp:  # fix
			# read chunk by chunk with tqdm to avoid memory exhaustion
			progress = tqdm.tqdm(range(file_size), \
								 "Receiving {}".format(f_name), \
								 unit="B", \
								 unit_scale=True, \
								 unit_divisor=1024)
			recv_len = 0
			for _ in progress:
				data = conn.recv(min(4096, file_size - recv_len))
				recv_len += len(data)
				if len(data) != fp.write(data):
					print("Error writing to file")
					break
				progress.update(len(data))
				del data  # hopefully release memory
				if recv_len == file_size:
					break
		return 1 if recv_len == file_size else 0

	def send_file(self, f_name, f_path, conn):
		filesize = os.path.getsize(f_path)
		# ack = {"type": "ack"}
		# ack["response_code"] = 0
		# ack["response"] = filesize
		# conn.sendall(json.dumps(ack).encode())
		with open(f_path, "rb") as fp:
			# send chunk by chunk with tqdm to avoid memory exhaustion
			print("Sending file: {0} to client having size: {1}".format(f_name, filesize))
			progress = tqdm.tqdm(range(filesize), \
								 "Sending {}".format(f_name), \
								 unit="B", \
								 unit_scale=True, \
								 unit_divisor=1024)
			send_len = 0
			for _ in progress:
				data = fp.read(min(4096, filesize - send_len))
				conn.sendall(data)
				send_len += len(data)
				progress.update(len(data))
				del data  # hopefully release memory

				if send_len == filesize:
					# read file and send success
					return

	#########################################################################################################################################################
	#########################################################################################################################################################

	#
	# receive all json data from socket
	#
	def recv_json_from_sock(self, sock):
		# sock	: tcp socket from which to retrieve json data

		# use stack to match {} to see if received all json data
		stack = 0
		data = []
		start_flag = True
		while True:
			tmp = sock.recv(1).decode("UTF-8")
			# check if received any data
			if len(tmp) != 1:
				return -1

			data.append(tmp)

			if not start_flag and tmp == '{':
				stack += 1
			elif tmp == '}':
				stack -= 1
			elif start_flag:
				# assuming that the message is always a dumped dictionary, thus starting with {
				stack += 1
				start_flag = False

			# all large paranthesises matched, done
			if stack == 0:
				break

		return "".join(data)

	#
	# one time send utility, for sending election and elected msg
	#
	def one_time_tcp_send(self, addr, msg, conn_timeout):
		# addr			: address in (IP, port) tuple
		# msg			: message in bytes
		# conn_timeout	: tcp connection timeout

		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

		sock.settimeout(conn_timeout)
		try:
			sock.connect(addr)
		except socket.timeout:
			print("Connection timeout")
			return -1
		except ConnectionRefusedError:
			print("Connection refused")
			return -1

		# timeout applies to sendall, too
		# will not cause any problem since message should be small
		sock.sendall(msg)
		sock.close()

		return 0

	#
	# get next member in membership list (in sorted order)
	#
	def get_next_member(self, ID, mem_list):
		# ID
		# mem_list	: membership list given by failure detector

		mem_list.sort()

		try:
			indx = mem_list.index(ID)
			ret = mem_list[(indx + 1) % len(mem_list)]
		except:
			print("ID:{} not found in list:{}".format(ID, mem_list))
			ret = -1

		return ret

	#
	# slave serve request main function
	# pass request msg to corresponding utility function
	#
	def slave_serve_request(self, cmd_msg, conn):
		# cmd_msg	: command message sent by client or master
		# conn		: client tcp connection
		with open("slave_log.log", "a") as fp: 
			if cmd_msg["type"] == "ret_file_ls":
				fp.write("{}\tstore command received. msg_content: {}\n".format(time.time(), cmd_msg))
				self.slave_retrieve_file_ls(cmd_msg, conn)
			elif cmd_msg["type"] == "put_file":
				fp.write("{}\tput_file command received. msg_content: {}\n".format(time.time(), cmd_msg))
				self.slave_put_file(cmd_msg, conn)
			elif cmd_msg["type"] == "get_file":
				fp.write("{}\tget_file command received. msg_content: {}\n".format(time.time(), cmd_msg))
				self.slave_get_file(cmd_msg, conn)
			elif cmd_msg["type"] == "del_file":
				fp.write("{}\tdel_file command received. msg_content: {}\n".format(time.time(), cmd_msg))
				self.slave_del_file(cmd_msg, conn)
			elif cmd_msg["type"] == "get_master":
				self.slave_get_master(cmd_msg, conn)
			elif cmd_msg["type"] == "halt":
				self.slave_halt(cmd_msg, conn)
			elif cmd_msg["type"] == "get_replicate":
				fp.write("{}\tget_replicate command received. msg_content: {}\n".format(time.time(), cmd_msg))
				self.slave_get_replicate(cmd_msg, conn)

		# if not match any of this, ignore and return
		return

	#
	# slave serve get replicate request
	# get file from other slave a request by master
	#
	def slave_get_replicate(self, cmd_msg, conn):
		# cmd_msg	: command message sent by client or master
		# conn		: client tcp connection

		request_ret_file_ls = {"type": "ret_file_ls"}
		request_get_file = {"type": "get_file", "filename": cmd_msg["filename"]}  # get_file request to remote slave
		ack_master = {"type": "ack", "response_code": 1}  # get_replicate ack to master

		# check if ip is self, return done immediate if is
		if cmd_msg["ip"] == socket.gethostname():
			if cmd_msg["filename"] in self.file_ls:
				ack_master["response_code"] = 0
			conn.sendall(json.dumps(ack_master).encode())
			return

		# initiate connection
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.settimeout(self.slave_cmd_timeout)

		addr = (cmd_msg["ip"], self.slave_listen_port)
		try:
			sock.connect(addr)
		except (socket.timeout, ConnectionRefusedError):
			print("Connection error")
			conn.sendall(json.dumps(ack_master).encode())  # notify master error
			return

		# send ret_file_ls, don't timeout afterwards
		sock.settimeout(None)
		sock.sendall(json.dumps(request_ret_file_ls).encode())

		# read reply
		ack = self.recv_json_from_sock(sock)
		sock.close()
		if ack == -1:
			conn.sendall(json.dumps(ack_master).encode())  # notify master error
			return

		ack = json.loads(ack)
		if ack["response_code"] == 1:
			conn.sendall(json.dumps(ack_master).encode())  # notify master error
			return

		# check if such file exists on remote slave
		file_ver = -1
		for elt in ack["response"]:
			if elt["filename"] == cmd_msg["filename"]:
				file_ver = elt["version"]
				break
		else:
			# no such file exists
			conn.sendall(json.dumps(ack_master).encode())
			return

		# file exists, get file
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.settimeout(self.slave_cmd_timeout)
		try:
			sock.connect(addr)
		except (socket.timeout, ConnectionRefusedError):
			print("Connection error")
			conn.sendall(json.dumps(ack_master).encode())  # notify master error
			return

		# send get_file, don't timeout afterwards
		sock.settimeout(None)
		sock.sendall(json.dumps(request_get_file).encode())

		# read reply
		ack = self.recv_json_from_sock(sock)
		if ack == -1:
			sock.close()
			conn.sendall(json.dumps(ack_master).encode())  # notify master error
			return

		ack = json.loads(ack)
		if ack["response_code"] == 1:
			sock.close()
			conn.sendall(json.dumps(ack_master).encode())  # notify master error
			return

		# first store file temproray
		tmp_file_path = os.path.join(self.slave_store_dir, "tmp_file")
		print("Start recv file")
		recv_rst = self.recv_file(cmd_msg["filename"], ack["response"], tmp_file_path, sock)
		print("End recv file")
		sock.close()
		if recv_rst == 0:
			# error writing file
			conn.sendall(json.dumps(ack_master).encode())
			return

		# decide target file name locally
		target_file_name = str(self.local_file_indx)
		if cmd_msg["filename"] in self.file_ls:
			# check if already stored
			target_file_name = self.file_ls[cmd_msg["filename"]]["real"]
		target_file_path = os.path.join(self.slave_store_dir, target_file_name)

		# rename file and update state
		os.rename(tmp_file_path, target_file_path)
		self.file_ls[cmd_msg["filename"]] = {"real": target_file_name, "version": file_ver}
		if target_file_name == str(self.local_file_indx):
			self.local_file_indx += 1

		# send back ack to master
		ack_master["response_code"] = 0
		conn.sendall(json.dumps(ack_master).encode())

		return

	#
	# slave serve retrieve file list request
	# send file list and version back to client
	#
	def slave_retrieve_file_ls(self, cmd_msg, conn):
		# cmd_msg	: command message sent by client or master
		# conn		: client tcp connection

		ack = {"type": "ack"}
		ack["response_code"] = 0
		ack["response"] = [{"filename": key, "version": value["version"]} for key, value in self.file_ls.items()]
		conn.sendall(json.dumps(ack).encode())

		return

	#
	# slave serve put file request
	# store file, update version number, send ack back to client
	#
	def slave_put_file(self, cmd_msg, conn):
		# cmd_msg	: command message sent by client or master
		# conn		: client tcp connection

		ack = {"type": "ack"}

		target_file_name = str(self.local_file_indx)
		if cmd_msg["filename"] in self.file_ls:
			# check if already stored
			target_file_name = self.file_ls[cmd_msg["filename"]]["real"]

		with open(os.path.join(self.slave_store_dir, target_file_name), "wb") as fp:

			# read chunk by chunk with tqdm to avoid memory exhaustion
			print("Receiving file: {} from client having size: {}".format(cmd_msg["filename"], cmd_msg["size"]))
			progress = tqdm.tqdm(range(cmd_msg["size"]), \
								 "Receiving {}".format(cmd_msg["filename"]), \
								 unit="B", \
								 unit_scale=True, \
								 unit_divisor=1024)

			recv_len = 0
			for _ in progress:
				# recv from socket and write chunk
				data = conn.recv(min(4096, cmd_msg["size"] - recv_len))
				recv_len += len(data)

				if len(data) != fp.write(data):
					print("Error writing to file")
					break

				progress.update(len(data))
				del data  # hopefully release memory

				if recv_len == cmd_msg["size"]:
					# recv and write to file success, update system stat to record the update
					self.file_ls[cmd_msg["filename"]] = {"real": target_file_name, "version": cmd_msg["version"]}

					if target_file_name == str(self.local_file_indx):
						self.local_file_indx += 1

					ack["response_code"] = 0
					conn.sendall(json.dumps(ack).encode())
					return

		# wrote to file failed, return error
		ack["response_code"] = 1
		conn.sendall(json.dumps(ack).encode())

		return

	#
	# slave serve get file request
	# send file back to client
	#
	def slave_get_file(self, cmd_msg, conn):
		# cmd_msg	: command message sent by client or master
		# conn		: client tcp connection

		ack = {"type": "ack"}

		# read and send back file if exists
		if cmd_msg["filename"] in self.file_ls:

			target_path = os.path.join(self.slave_store_dir, self.file_ls[cmd_msg["filename"]]["real"])
			filesize = os.path.getsize(target_path)

			# send back ack first
			ack["response_code"] = 0
			ack["response"] = filesize
			conn.sendall(json.dumps(ack).encode())

			with open(target_path, "rb") as fp:

				# send chunk by chunk with tqdm to avoid memory exhaustion
				print("Sending file: {} to client having size: {}".format(cmd_msg["filename"], filesize))
				progress = tqdm.tqdm(range(filesize), \
									 "Sending {}".format(cmd_msg["filename"]), \
									 unit="B", \
									 unit_scale=True, \
									 unit_divisor=1024)

				send_len = 0
				for _ in progress:

					data = fp.read(min(4096, filesize - send_len))

					conn.sendall(data)
					send_len += len(data)

					progress.update(len(data))
					del data  # hopefully release memory

					if send_len == filesize:
						# read file and send success
						return

		ack["response_code"] = 1
		conn.sendall(json.dumps(ack).encode())

		return

	#
	# slave serve delete file request
	# delete file and ack client
	#
	def slave_del_file(self, cmd_msg, conn):
		# cmd_msg	: command message sent by client or master
		# conn		: client tcp connection

		ack = {"type": "ack"}

		# check if file exist and delete it (update stat along the way)
		if cmd_msg["filename"] in self.file_ls:
			os.remove(os.path.join(self.slave_store_dir, self.file_ls[cmd_msg["filename"]]["real"]))
			del self.file_ls[cmd_msg["filename"]]

		ack["response_code"] = 0

		conn.sendall(json.dumps(ack).encode())

		return

	#
	# slave serve get master request
	# send master back to client
	#
	def slave_get_master(self, cmd_msg, conn):
		# cmd_msg	: command message sent by client or master
		# conn		: client tcp connection

		ack = {"type": "ack"}
		ack["response_code"] = 0
		ack["response"] = self.master

		conn.sendall(json.dumps(ack).encode())

		return

	#
	# slave serve get master request
	# send master back to client
	#
	def slave_halt(self, cmd_msg, conn):
		# cmd_msg	: command message sent by client or master
		# conn		: client tcp connection
		ack = {"type": "ack"}
		ack["response_code"] = 0

		conn.sendall(json.dumps(ack).encode())
		self.run = 0
		self.master_run = 0
		return

	#
	# SDFS slave init function
	# Cleans up local directory and set local variables for future execution
	# Run before slave_run()
	#
	def slave_init(self, slave_listen_port, slave_store_dir, slave_cmd_timeout, slave_election_timeout, ID,
				   slave_introducer):
		# slave_listen_port		: port on which slave will listen for local command
		# slave_store_dir		: directory path where slave will store the received replicas
		# slave_cmd_timeout		: recv timeout time when during command execution
		# slave_election_timeout: recv timeout time when during election
		# ID					: ID for this self node

		self.slave_listen_port = slave_listen_port
		self.slave_store_dir = slave_store_dir
		self.slave_cmd_timeout = slave_cmd_timeout
		self.slave_election_timeout = slave_election_timeout
		self.ID = ID
		self.slave_introducer = slave_introducer

		if os.path.isdir(self.slave_store_dir):
			shutil.rmtree(self.slave_store_dir)
		os.mkdir(self.slave_store_dir)

		# ask introducer for master IP
		# set to -1 if failed
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.settimeout(50)
		try:
			request = {"type": "get_master"}
			sock.connect((slave_introducer, self.slave_listen_port))
			sock.sendall(json.dumps(request).encode())  # timeout applies, but should not do harm
			ack = self.recv_json_from_sock(sock)
			sock.close()

			print("ack: {}".format(ack))
			# error getting response
			if ack == -1:
				raise socket.timeout

			# error in response
			ack = json.loads(ack)
			if ack["response_code"] == -1:
				raise socket.timeout

			self.master = ack["response"]
		except (socket.timeout, ConnectionRefusedError):
			print("Error contacting introducer, setting master to -1")
			self.master = -1
			sock.close()

		# local file storage related variables
		self.file_ls = {}
		self.local_file_indx = 0

	def slave_run(self):
		'''
		SDFS slave main thread
		Serve commands from master node and clients indefinitely
		Start election process if master node detected to be failed by failure detection component
		TODO
		- should add functionality that when in election, prevent election message from lower initiator from traversing ring
		:return:
		'''
		self.run = 1
		need_init_flag = True
		master_thread = None
		print("Enter Slave Loop")
		while self.run:
			# always check at first if failure_detector is running
			if self.fd.hb_on and need_init_flag:
				# failure detector online! init and start normal operation
				# init all parameters stay the same, but will perform all things again
				slave_listen_port = self.slave_listen_port
				slave_store_dir = self.slave_store_dir
				slave_cmd_timeout = self.slave_cmd_timeout
				slave_election_timeout = self.slave_election_timeout
				slave_introducer = self.slave_introducer

				self.slave_init(slave_listen_port, \
								slave_store_dir, \
								slave_cmd_timeout, \
								slave_election_timeout, \
								self.fd.self_node.id, \
								slave_introducer)

				welcome_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				welcome_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				welcome_sock.bind(("0.0.0.0", self.slave_listen_port))
				welcome_sock.listen(5)
				cur_timeout = self.slave_cmd_timeout

				need_init_flag = False
			elif not self.fd.hb_on and not need_init_flag:
				# failure detector not on somehow, do not operate
				welcome_sock.close()
				need_init_flag = True
				continue
			elif not self.fd.hb_on and need_init_flag:
				# failure detector not on and need init, periodic checking
				time.sleep(1)  # sleep for some time before checking failure detector again
				continue

			# normal operations
			# set timeout and accept
			welcome_sock.settimeout(cur_timeout)

			# get membership and next hop on election ring
			mem_list = self.fd.ret_membership_id_ls()
			next_member = self.get_next_member(self.ID, mem_list)

			try:
				conn, _ = welcome_sock.accept()

				data = self.recv_json_from_sock(conn)
				if data == -1:
					print("receive request failed")
					conn.close()
					continue

				request = json.loads(data)
				print("Received msg: {}".format(request))
				if request["type"] == "election":
					# remove last election result since someone did not get it
					# stop the master
					self.master = -1
					if self.master_run == 1:
						self.master_run = 0
						while self.master_dead == 0:
							print("???")
							time.sleep(0.1)

					if request["candidate"] == self.ID:
						# election finished, send elected
						request["type"] = "elected"
						self.one_time_tcp_send((next_member.split(sep=" ")[0], self.slave_listen_port), \
											   json.dumps(request).encode(), \
											   cur_timeout)
						cur_timeout = self.slave_cmd_timeout
						self.master = self.ID

						# start master thread
						print("I am the new master lol")
						self.__init__(self.client_listen_port)  # init function works for master only
						self.new_master = 1
						self.master_run = 1
						self.master_dead = 0
						master_thread = threading.Thread(target=self.master_loop, daemon=True)
						master_thread.start()

					else:
						# election continues
						if request["candidate"] < self.ID:
							# update election message if needed
							request["candidate"] = self.ID

						if request["candidate"] in mem_list:
							# check in election candidate is alive
							self.one_time_tcp_send((next_member.split(sep=" ")[0], self.slave_listen_port), \
												   json.dumps(request).encode(), \
												   cur_timeout)
							cur_timeout = self.slave_election_timeout + random.randrange(0, 6)

				elif request["type"] == "elected":
					if request["candidate"] != self.ID:
						if request["candidate"] in mem_list:
							# got elected message, check if alive and update accordingly
							self.one_time_tcp_send((next_member.split(sep=" ")[0], self.slave_listen_port), \
												   json.dumps(request).encode(), \
												   cur_timeout)
							cur_timeout = self.slave_cmd_timeout
							self.master = request["candidate"]

				else:
					if self.master != -1 or request["type"] == "halt":
						# serve request as in normal operation
						# halt should still be served when no master
						self.slave_serve_request(request, conn)

					if request["type"] == "halt":
						# end of sdfs slave operation
						conn.close()
						break

				# done with connection
				conn.close()

			except socket.timeout:
				# got timeout
				if type(self.master) != str and len(mem_list) != 1:
					# timeout due to election, start election
					request = {"type": "election", "candidate": self.ID, "initiator": self.ID}
					if self.master_run == 1:
						self.master_run = 0
						while self.master_dead == 0:
							time.sleep(0.1)
					self.one_time_tcp_send((next_member.split(sep=" ")[0], self.slave_listen_port),
										   json.dumps(request).encode(),
										   cur_timeout)

			# check failure of master when master assigned
			if self.master != -1:
				if not self.master in mem_list:
					# master failed
					self.master = -1
					cur_timeout = self.slave_election_timeout + random.randrange(0, 6)

		# end of slave operation
		welcome_sock.close()

		print('Halting SDFS')

	def UI(self):
		self.fd, t2 = RingFailureDetector(int(sys.argv[3]), queue.Queue()), 0
		t = threading.Thread(target=self.fd.failure_detection, daemon=True)
		t.start()
		while True:
			print("\nAvailable commands: ")
			print("\tintroducer")
			print("\tjoin")
			print("\tleave")
			print("\tmembership list")
			print("\tmembership list id")
			print("\tself id")
			print("\tget table")
			print("\tfailure list")
			print("\treverse list")
			print("\texit")
			cmd = input("Type in the command: ")

			# check and notify main thread of cmd received
			if cmd == "introducer" or \
				cmd == "join" or \
				cmd == "leave" or \
				cmd == "membership list" or \
				cmd == "membership list id" or \
				cmd == "self id" or \
				cmd == "get table" or \
				cmd == "failure list" or \
				cmd == "reverse list" or \
				cmd == "exit":
				self.fd.cmd_queue.put(cmd)

				if cmd == 'introducer' or cmd == 'join':
					if t2:
						print('SDFS already running')
						continue

					time.sleep(self.start_fd_wait)
					self.slave_init(int(sys.argv[2]), \
									"tmp", \
									3, \
									5, \
									socket.gethostname() + " 0", \
									"fa19-cs425-g43-01.cs.illinois.edu")
					t2 = threading.Thread(target=self.slave_run, daemon=True)
					t2.start()
			else:
				print('No such command. Try again.')
				continue

			if cmd == "exit":
				return

if __name__ == "__main__":

	if len(sys.argv) != 4:
		print("Usage: python3 {} MASTER_PORT SLAVE_PORT FD_PORT".format(sys.argv[0]))
		exit(1)

	server = sdfsfilesystem(int(sys.argv[1]))
	print("PORTTTT {}".format(int(sys.argv[1])))
	t1 = threading.Thread(target=server.UI)
	t1.start()