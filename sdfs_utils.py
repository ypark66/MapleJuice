#!/usr/bin/python3
import socket
import os
import json
import tqdm

def recv_json_from_sock_block(sock, block_size):
	'''
	:Function for receiving the whole json message from a tcp socket in block size.
	:sock: tcp socket from which to retrieve json data.
	:block_size: data block read size.
	:Returns a json string (type string).
	'''

	# use stack to match {} to see if received all json data
	lp = int.from_bytes(b"{", "big")
	rp = int.from_bytes(b"}", "big")
	stack = 0
	data = bytearray()
	end_flag = False
	while not end_flag:
		try:
			tmp = sock.recv(block_size)
		except Exception as e:
			print(e)
			return None

		# assuming that the message is always a dumped dictionary, thus starting with {
		extend_len = 0
		for i in tmp:
			if i == lp:
				stack += 1
			elif i == rp:
				stack -= 1

			extend_len += 1

			# all large paranthesises matched, done
			if stack == 0:
				end_flag = True
				break

		data.extend(tmp[:extend_len])

	return bytes(data).decode()

def recv_json_from_sock(sock):
	'''
	:Function for receiving the whole json message from a tcp socket.
	:sock: tcp socket from which to retrieve json data.
	:Returns a json string (type string).
	'''

	# use stack to match {} to see if received all json data
	stack = 0
	data = []
	while True:
		tmp = sock.recv(1).decode()
		if len(tmp) != 1:
			return None

		data.append(tmp)

		# assuming that the message is always a dumped dictionary, thus starting with {
		if tmp == '{':
			stack += 1
		elif tmp == '}':
			stack -= 1

		# all large paranthesises matched, done
		if stack == 0:
			break

	return "".join(data)

def send_file(filename, filesize, file_content, conn):
	'''
	:function for sending file to a connection (usually to SDFS).
	:filename: string, sending filename, only used to show progress bar.
	:filesize: integer, sending filesize in bytes.
	:file_content: bytes, sending file content.
	:conn: connection to which to send data.
	:return True if error or False if success.
	'''

	print("Sending file: {0} having size: {1}".format(filename, filesize))
	progress = tqdm.tqdm(range(filesize), \
							"Sending {}".format(filename), \
							unit="B", \
							unit_scale=True, \
							unit_divisor=1024)

	send_len = 0
	for _ in progress:
		target_len = min(4096, filesize - send_len)

		try:
			conn.sendall(file_content[send_len:send_len + target_len])
		except:
			return True
		
		send_len += target_len
		progress.update(target_len)

		if send_len == filesize:
			break

	return False

def recv_file(filename, filesize, conn):
	'''
	:function for receiving file from a connection (usually from SDFS slave).
	:filename: string, receiving filename, only used to show progress bar.
	:filesize: integer, receiving filesize in bytes.
	:conn: connection from which to receive data.
	:return the file content in bytes or None.
	'''

	print("Receiving file: {} having size: {}".format(filename, filesize))
	progress = tqdm.tqdm(range(filesize), \
							"Receiving {}".format(filename), \
							unit="B", \
							unit_scale=True, \
							unit_divisor=1024)
	
	recv_len = 0
	data = bytearray()
	for _ in progress:
		try:
			tmp = conn.recv(min(4096, filesize - recv_len))
		except:
			return None

		recv_len += len(tmp)
		data.extend(tmp)
		progress.update(len(tmp))

		if recv_len == filesize:
			break

	return bytes(data)

def get_master(sdfs_slave_port):
	'''
	:Get current master IP address.
	:sdfs_master_port: integer, port that the sdfs master is listening on.
	:sdfs_slave_port: integer, port that the sdfs slave is listening on.
	:return either None or current IP address of master (string).
	'''

	ip_list = ['fa19-cs425-g43-01.cs.illinois.edu',
				'fa19-cs425-g43-02.cs.illinois.edu',
				'fa19-cs425-g43-03.cs.illinois.edu',
				'fa19-cs425-g43-04.cs.illinois.edu',
				'fa19-cs425-g43-05.cs.illinois.edu',
				'fa19-cs425-g43-06.cs.illinois.edu',
				'fa19-cs425-g43-07.cs.illinois.edu',
				'fa19-cs425-g43-08.cs.illinois.edu',
				'fa19-cs425-g43-09.cs.illinois.edu',
				'fa19-cs425-g43-10.cs.illinois.edu']
				
	msg = json.dumps({'type':'get_master'}).encode()
	master_ip = None

	while ip_list:
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.settimeout(1)

		server_address = (ip_list.pop(), sdfs_slave_port)
		try:
			sock.connect(server_address)
		except:
			continue

		data = ""
		try:
			sock.sendall(msg)
			data = sock.recv(1024)
			data = json.loads(data)
			if data['response_code'] == 1:
				continue
			else:
				master_ip = data['response'].split(' ')[0]
				break
		except:
			pass
		finally:
			sock.close()

	return master_ip

def get_file_from_sdfs(sdfs_filename, sdfs_master_port, sdfs_slave_port):
	'''
	:Get file from sdfs.
	:sdfs_filename: string, filename of target file in sdfs system.
	:sdfs_master_port: integer, port that the sdfs master is listening on.
	:sdfs_slave_port: integer, port that the sdfs slave is listening on.
	:return either None or targte file content (bytes).
	'''

	sdfs_master_ip = get_master(sdfs_slave_port)
	if sdfs_master_ip is None:
		return None

	msg = json.dumps({'type': 'get_file', 'filename': sdfs_filename}).encode()
	
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	try:
		sock.connect((sdfs_master_ip, sdfs_master_port))
	except:
		return None

	data = None
	try:
		sock.sendall(msg)
		data = sock.recv(1024)
		data = json.loads(data)
		if data["response_code"] == 1:
			raise Exception("Response code is 1")

		data = data['response']
	except:
		sock.close()
		return None
	sock.close()

	# receive file from the slave
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	try:
		sock.connect((data, sdfs_slave_port))
	except:
		return None

	rst = None
	try:
		sock.sendall(msg)
		data = sock.recv(1024)
		data = json.loads(data)
		if data["response_code"] == 1:
			raise Exception("Response code is 1")

		size = data['response']
		rst = recv_file(sdfs_filename, size, sock)
	except Exception as e:
		print(e)
	finally:
		sock.close()

	return rst

def store_file_to_sdfs(sdfs_filename, file_content, sdfs_master_port, sdfs_slave_port):
	'''
	:Store file into sdfs.
	:sdfs_filename: string, filename to store in sdfs system.
	:file_content: bytes, file content to be stored in sdfs.
	:sdfs_master_port: integer, port that the sdfs master is listening on.
	:sdfs_slave_port: integer, port that the sdfs slave is listening on.
	:return either True if fault occurred or False if success.
	'''

	filesize = len(file_content)
	msg = json.dumps({'type': 'put_file', 'size': filesize, 'filename': sdfs_filename}).encode()
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	try:
		sock.connect((get_master(sdfs_slave_port), sdfs_master_port))
	except Exception as e:
		print(e)
		return True

	data = ""
	try:
		sock.sendall(msg)
		data = sock.recv(1024)
		data = json.loads(data)

		# the server may notify 60 sec write, deal with it
		if data["type"] == 1:
			sock.sendall(json.dumps({"type": 1}).encode())
				
		if send_file(sdfs_filename, filesize, file_content, sock):
			raise Exception()

		data = sock.recv(1024)
		data = json.loads(data)
		if data["response_code"] == 1:
			raise Exception()

	except:
		sock.close()
		return True

	sock.close()
	return False

def del_file_of_sdfs(sdfs_filename, sdfs_master_port, sdfs_slave_port):
	'''
	:Del file from sdfs.
	:sdfs_filename: string, filename of target file in sdfs system.
	:sdfs_master_port: integer, port that the sdfs master is listening on.
	:sdfs_slave_port: integer, port that the sdfs slave is listening on.
	:return True if fault happened or False if not.
	'''

	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.settimeout(50)

	master_ip = get_master(sdfs_slave_port)
	if master_ip is None:
		return True

	request = json.dumps({"type": "del_file", 'filename': sdfs_filename}).encode()
	try:
		sock.connect((master_ip, sdfs_master_port))
	except:
		return True
	
	try:
		sock.sendall(request)
		data = sock.recv(1024)
		data = json.loads(data)
		if data["response_code"] == 1:
			raise Exception()
	except:
		sock.close()
		return True

	sock.close()		
	return False

def get_file_ls_from_sdfs(sdfs_master_port, sdfs_slave_port):
	'''
	:Get file list of sdfs.
	:sdfs_master_port: integer, port that the sdfs master is listening on.
	:sdfs_slave_port: integer, port that the sdfs slave is listening on.
	:return filename list (list of string) or None if not.
	'''

	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.settimeout(50)

	master_ip = get_master(sdfs_slave_port)
	if master_ip is None:
		return None

	request = json.dumps({"type": "get_file_list"}).encode()
	try:
		sock.connect((master_ip, sdfs_master_port))
	except:
		return None
	
	try:
		sock.sendall(request)
		data = recv_json_from_sock(sock)
		if data is None:
			raise Exception()

		data = json.loads(data)
		if data["response_code"] == 1:
			raise Exception()
	except:
		sock.close()
		return None

	sock.close()
	return data["response"]