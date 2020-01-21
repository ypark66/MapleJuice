#!/usr/bin/python3
import sys
import time
import random
import json
import base64
import queue
import threading
import hashlib
from failure_detector import RingFailureDetector
from sdfs_utils import *

class MapleJuiceMaster:
	def __init__(self, sdfs_master_port, sdfs_slave_port, mj_port, fd_port, client_port, shuffle_type):
		self.sdfs_master_port = sdfs_master_port
		self.sdfs_slave_port = sdfs_slave_port
		self.mj_port = mj_port
		self.fd_port = fd_port
		self.client_port = client_port
		self.shuffle_type = shuffle_type
		self.listen_timeout = 1
		self.max_get_file_trials = 50
		self.max_notify_trials = 5
		self.max_assign_retry = 5
		self.get_file_wait = 3
		self.get_file_ls_wait = 3
		self.notify_wait = 3
		self.retry_assign_wait = 3
		self.fd_start_wait = 3
		self.fd_cmd_queue = queue.Queue()
		self.fd = RingFailureDetector(self.fd_port, self.fd_cmd_queue)
		self.request_cnt = 0
		self.task_cnt = 0
		self.request_q_lock = threading.Lock()
		self.request_queue = []
		self.serving = False
		self.log_path = "mj_master.log"

		if self.shuffle_type != "0" and self.shuffle_type != "1":
			raise ValueError("Invalid shuffle type value. Expect 0 or 1, got {}".format(self.shuffle_type))

		return

	def cal_md5(self, key):
		'''
		:Utility for calculating MD5 sum of a given string.
		:key: string.
		:Return an integer corresponding to the MD5 hash of the key.
		'''
		return int(hashlib.md5(key.encode()).hexdigest(), base=16)

	def notify_client_error(self, request_ID, addr):
		# construct reply message
		notification = json.dumps({"type": "notify", \
									"response_code": 1, \
									"request_ID": request_ID}).encode()

		# notify client that the request is done
		for _ in range(self.max_notify_trials):
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

			try:
				sock.connect((addr, self.client_port))
			except Exception as e:
				print(e)
				time.sleep(self.notify_wait)
				continue

			try:
				sock.sendall(notification)

				data = recv_json_from_sock(sock)
				if data is None or len(data) == 0:
					raise ValueError()

				data = json.loads(data)
				if data["response_code"] == 1:
					raise Exception()
				
				break

			except Exception as e:
				print(e)
				sock.close()
				time.sleep(self.notify_wait)
				continue

	def get_worker_addr_in_IP(self):
		self.request_q_lock.acquire()
		rst = []
		for key in self.request_queue[0]["workers"]:
			rst.append((socket.gethostbyname(key), key))
		self.request_q_lock.release()	

		return dict(rst)

	def get_memlist_in_addr(self):
		'''
		:Function for getting list of members address from fd, excluding self.
		:Returns member address in the form of list of strings.
		'''
		mem_list = self.fd.ret_membership_id_ls()
		master_address = socket.gethostname()
		return [i.split(sep=" ")[0] for i in mem_list if i.split(sep=" ")[0] != master_address]

	def get_list_of_files_from_sdfs(self, file_list):
		'''
		:Function for getting content of all files in file_list.
		:file_list: list of strings storing target filenames.
		:Returns parsed (by newline character) file content in a list of strings.
		'''
		data = []
		for filename in file_list:
			
			# get file from sdfs for self.max_get_file_trials times before giving up
			for _ in range(self.max_get_file_trials):
				
				tmp = get_file_from_sdfs(filename, self.sdfs_master_port, self.sdfs_slave_port)
				if not tmp is None:
					break
				
				time.sleep(self.get_file_wait)
			else:
				return None

			data += [i.decode('utf-8') for i in tmp.split(sep=b"\n")]

		return data

	def parse_sdfsfilename(self, target, prefix):
		'''
		:Function for extracting key from sdfs filename.
		:target: string, target sdfs filename.
		:prefix: string, target sdfs filename prefix.
		:Returns parsed key if key found or None if prefix not matching.
		'''

		if len(target) <= len(prefix) + 1:
			return None

		if target[:len(prefix) + 1] != prefix + "_":
			return None
		
		return target[len(prefix) + 1:]

	def get_keys_from_sdfs(self, prefix):
		'''
		:Function for extracting keys for a particular prefix from sdfs.
		:prefix: string, target sdfs filename prefix.
		:Returns list of parsed keys, corresponding to prefix.
		'''

		file_ls = get_file_ls_from_sdfs(self.sdfs_master_port, self.sdfs_slave_port)
		if file_ls == None:
			return None

		rst = []
		for filename in file_ls:
			tmp = self.parse_sdfsfilename(filename, prefix)
			if not tmp is None:
				rst.append(tmp)

		return rst

	def serve_client_request_msg(self, msg, conn, addr):
		'''
		:Function for serving client requests by putting it onto the queue.
		:msg: dictionary object containing request message from client.
		:conn: connection of client.
		:addr: IP address or hostname of client.
		:Returns nothing.
		'''

		msg["request_ID"] = self.request_cnt
		self.request_cnt += 1
		msg["addr"] = addr

		self.request_q_lock.acquire()
		self.request_queue.append(msg)
		self.request_q_lock.release()

		ack = {"type": "ack", "response_code": 0, "request_ID": msg["request_ID"]}
		conn.sendall(json.dumps(ack).encode())

		# log job received
		with open(self.log_path, "a") as fp:
			fp.write("Received job: {}\n".format(msg))

		return

	def serve_worker_result(self, msg, conn, addr):
		'''
		:Function for serving worker results, will contact client if request completed.
		:msg: dictionary object containing request message from client.
		:conn: connection of client.
		:addr: IP address or hostname of client.
		:Returns nothing.
		'''
		worker_addr_dict = self.get_worker_addr_in_IP()
		print(worker_addr_dict)

		ack = {"type": "ack", "response_code": 0}
		conn.sendall(json.dumps(ack).encode())

		# maintain a dict of tasks for each worker assigned to this requests
		# task dict: key - task_ID, value - done_flag, input/keys, rst
		self.request_q_lock.acquire()
		
		# deal with worker failed, abort the request and notify client
		if msg["rst"] == 0:

			request_ID = self.request_queue[0]["request_ID"]
			addr = self.request_queue[0]["addr"]

			self.request_queue.pop(0)
			self.request_q_lock.release()
			self.serving = False
			self.notify_client_error(request_ID, addr)

			return


		print("serving result")

		# check if worker is associated with this request
		if addr in worker_addr_dict:
			
			addr = worker_addr_dict[addr]

			# check if task is assigned to this worker
			task_dict = self.request_queue[0]["workers"][addr]
			if msg["task_ID"] in task_dict:

				# check if task is already done
				if not task_dict[msg["task_ID"]]["done_flag"]:
					
					# task not done, done now
					task_dict[msg["task_ID"]]["done_flag"] = True # CHECK if will change value in org dict
					task_dict[msg["task_ID"]]["rst"] = msg["rst"]
					task_dict["tasks_done"] += 1

					# check if all tasks is done, worker is done if so
					if task_dict["tasks_done"] == len(task_dict) - 1:
						self.request_queue[0]["workers_done"] += 1

		# print(self.request_queue[0])

		# if worker are all done, the request is completed, do finalization
		if self.request_queue[0]["workers_done"] >= len(self.request_queue[0]["workers"]):
			# extract results from all workers
			request_rst = {}
			for worker in self.request_queue[0]["workers"]:
				for task in self.request_queue[0]["workers"][worker]:
					if task == "tasks_done":
						continue

					for key in self.request_queue[0]["workers"][worker][task]["rst"].keys():
						if not key in request_rst:
							request_rst[key] = []
						request_rst[key] += self.request_queue[0]["workers"][worker][task]["rst"][key]

			# store into SDFS
			if self.request_queue[0]["type"] == "maple_request":
				for key in request_rst.keys():
					tmp = json.dumps({key:request_rst[key]}).encode()
					while store_file_to_sdfs("{}_{}".format(self.request_queue[0]["prefix"], key), \
												tmp, \
												self.sdfs_master_port, \
												self.sdfs_slave_port):
						pass
			elif self.request_queue[0]["type"] == "juice_request":
				#tmp = json.dumps(request_rst).encode()
				key_ls = sorted(request_rst.keys())
				store_fmt = []
				for i in key_ls:
					store_fmt.append((i, request_rst[i]))
				store_fmt = json.dumps(store_fmt).encode()
				while store_file_to_sdfs(self.request_queue[0]["dest_filename"], \
											store_fmt, \
											self.sdfs_master_port, \
											self.sdfs_slave_port):
					pass
			
			# remove input file from sdfs if needed
			if self.request_queue[0]["type"] == "juice_request" and self.request_queue[0]["delete_input"]:
				for key in self.request_queue[0]["keys"]:
					while del_file_of_sdfs("{}_{}".format(self.request_queue[0]["prefix"], key), \
											self.sdfs_master_port, \
											self.sdfs_slave_port):
						pass

			# construct reply message
			notification = json.dumps({"type": "notify", \
										"response_code": 0, \
										"request_ID": self.request_queue[0]["request_ID"]}).encode()
			client_addr = self.request_queue[0]["addr"]

			# log job done
			with open(self.log_path, "a") as fp:
				fp.write("Job ID: {} Done\n".format(self.request_queue[0]["request_ID"]))

			# done with this request
			self.request_queue.pop(0)

			# notify client that the request is done
			for _ in range(self.max_notify_trials):
				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

				try:
					sock.connect((client_addr, self.client_port))
				except:
					time.sleep(self.notify_wait)
					continue

				try:
					sock.sendall(notification)

					data = recv_json_from_sock(sock)
					if data is None or len(data) == 0:
						raise ValueError()

					data = json.loads(data)
					if data["response_code"] == 1:
						raise Exception()

					break
				except:
					sock.close()
					time.sleep(self.notify_wait)
					continue

			# not serving now
			self.serving = False

		self.request_q_lock.release()

		return

	def serve_msg(self, msg, conn, addr):
		'''
		:Serve request main function.
		:msg: dictionary object containing request and reply message from client and worker.
		:Returns nothing.
		'''

		if msg["type"] == "maple_request" or msg["type"] == "juice_request":
			self.serve_client_request_msg(msg, conn, addr)
		elif msg["type"] == "result":
			if self.serving:
				self.serve_worker_result(msg, conn, addr)
			else:
				del msg["rst"]
				print("Got reply while not serving any request, weird: {}".format(msg))
		else:
			print("Ignoring unrecognized message: {}".format(msg))

		return

	def send_assignment(self, worker_addr, request_type, task_ID, exe, worker_input=None, prefix=None, keys=None):
		'''
		:Function for sending assignments to workers.
		:worker_addr: string, address for target worker.
		:request_type: string, to specify what kind of request this assignment is for. Either "maple_request" or "juice_request".
		:task_ID: integer, task ID for this assigned tasks.
		:exe: string, base64 encoded exe for maple or juice function.
		:worker_input: list of strings, input lines for maple requests.
		:prefix: string, SDFS file prefix for juice requests.
		:keys: list of strings, SDFS file keys for juice requests.
		:Returns False if success, True if not.
		'''

		# prepare message
		assign = {"task_ID":task_ID}
		if request_type == "maple_request":
			assign["type"] = "assign_maple"
			assign["maple_exe"] = exe
			
			if worker_input == None:
				raise ValueError("input should not be None for maple task assignment")
			assign["input"] = worker_input
		elif request_type == "juice_request":
			assign["type"] = "assign_juice"
			assign["juice_exe"] = exe

			if prefix == None:
				raise ValueError("prefix should not be None for juice task assignment")
			assign["prefix"] = prefix

			if keys == None:
				raise ValueError("keys should not be None for juice task assignment")
			assign["keys"] = keys
		else:
			raise ValueError("Invalid request_type value, should be \"maple_request\" or \"juice_request\"")
		assign = json.dumps(assign).encode()

		# contact worker
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		try:
			sock.connect((worker_addr, self.mj_port))
		except Exception as e:
			print(e)
			return True

		try:
			sock.sendall(assign)

			data = recv_json_from_sock(sock)
			if data is None or len(data) == 0:
				raise ValueError("Data NULL")

			data = json.loads(data)
			if data["response_code"] == 1:
				raise Exception("Error Response")
		except Exception as e:
			print(e)
			sock.close()
			return True

		return False

	def send_ls_assignment(self, worker_addr, request_type, exe, task_dict, prefix=None):
		'''
		:Function for assigning a list of tasks to a worker.
		:worker_addr: string, Address for target worker.
		:request_type: string, To specify what kind of request this assignment is for. Either "maple_request" or "juice_request".
		:exe: string, base64 encoded Exe for maple or juice function.
		:task_dict: dictionary of tasks
		:prefix: string, SDFS file prefix for juice requests.
		:Returns False if success, True if not.
		'''

		rst = False
		for key in task_dict:
			if key == "tasks_done":
				continue

			if task_dict[key]["done_flag"]: # already done, no need to assign
				continue
			
			for _ in range(self.max_assign_retry):
				if request_type == "maple_request":
					if self.send_assignment(worker_addr, \
											request_type, \
											key, \
											exe, \
											worker_input=task_dict[key]["input"]):
						continue
					break
				elif request_type == "juice_request":
					if self.send_assignment(worker_addr, \
											request_type, \
											key, \
											exe, \
											worker_input=None, \
											prefix=prefix, \
											keys=task_dict[key]["keys"]):
						continue
					break
				else:
					raise ValueError("Invalid request_type value, should be \"maple_request\" or \"juice_request\"")
			else:
				rst = True
				break
			
		return rst

	def serve_queued_request(self):
		'''
		Function for serving request for the first time.
		Returns nothing.
		'''
		self.serving = True

		self.request_q_lock.acquire()
		request_type = self.request_queue[0]["type"]
		worker_num = self.request_queue[0]["worker_num"]
		request_ID = self.request_queue[0]["request_ID"]
		if request_type == "maple_request":
			exe = self.request_queue[0]["maple_exe"]
			file_list = self.request_queue[0]["file_list"].copy()
		elif request_type == "juice_request":
			exe = self.request_queue[0]["juice_exe"]
			prefix = self.request_queue[0]["prefix"]
		else:
			# unknow request, remove it
			request_ID = self.request_queue[0]["request_ID"]
			addr = self.request_queue[0]["addr"]

			self.request_queue.pop(0)
			self.request_q_lock.release()
			self.serving = False
			self.notify_client_error(request_ID, addr)
			return
		self.request_q_lock.release()

		# use old_mem_list as assignment reference so that check_n_assign 
		# can detect that an worker has died 
		# if old_mem_list is empty, return and wait until there is at least one member on it

		mem_list = self.old_mem_list

		avail_worker_num = min(worker_num, len(mem_list))
		if avail_worker_num == 0:
			# no worker available, don't serve yet
			self.serving = False
			return

		print("Start serving")

		# log job scheduled
		with open(self.log_path, "a") as fp:
			fp.write("Scheduling job with ID: {}\n".format(request_ID))

		random.shuffle(mem_list)
		worker_list = {}
		print("Post mem_list")

		# for maple, keep on trying until get all data
		if request_type == "maple_request":
			while True:
				data = self.get_list_of_files_from_sdfs(file_list)
				if not data is None:
					break
				time.sleep(self.get_file_wait)
		else:
			# retrieve key list for juice request 
			while True:
				keys = self.get_keys_from_sdfs(prefix)
				if not keys is None:
					break
				time.sleep(self.get_file_ls_wait)
			
			if self.shuffle_type == "1": # range shuffling
				keys.sort()
			
			print("Master got keys: {}".format(keys))

			if len(keys) == 0:
				# there should be at least one key in sdfs but no keys found
				self.request_q_lock.acquire()
				request_ID = self.request_queue[0]["request_ID"]
				addr = self.request_queue[0]["addr"]

				self.request_queue.pop(0)
				self.request_q_lock.release()
				self.serving = False
				self.notify_client_error(request_ID, addr)
				return
		print("Post get data")

		if request_type == "maple_request" or self.shuffle_type == "1":
			if request_type == "maple_request":
				if len(data) % avail_worker_num == 0:
					portion_len = len(data) // avail_worker_num
				else:
					portion_len = len(data) // avail_worker_num + 1
			else:
				if len(keys) % avail_worker_num == 0:
					portion_len = len(keys) // avail_worker_num
				else:
					portion_len = len(keys) // avail_worker_num + 1

			for i in range(avail_worker_num):
				worker_addr = mem_list[i]
				worker_list[worker_addr] = {"tasks_done":0}
				if request_type == "maple_request":
					worker_list[worker_addr][self.task_cnt] = {"done_flag":False, \
																"input":data[i * portion_len:(i+1) * portion_len]}
				else:
					worker_list[worker_addr][self.task_cnt] = {"done_flag":False, \
																"keys":keys[i * portion_len:(i+1) * portion_len]}
				self.task_cnt += 1
		else: # self.shuffle_type == "0"
			tmp_store = {}
			for key in keys:
				hash_indx = self.cal_md5(key) % len(mem_list)
				if not mem_list[hash_indx] in tmp_store:
					tmp_store[mem_list[hash_indx]] = []
				tmp_store[mem_list[hash_indx]].append(key)

			for worker_addr in tmp_store:
				worker_list[worker_addr] = {"tasks_done":0}
				worker_list[worker_addr][self.task_cnt] = {"done_flag":False, \
															"keys":tmp_store[worker_addr]}
				self.task_cnt += 1
		print("Post dist input/keys")

		# send assignment to workers
		for worker_addr in worker_list:
			# find task ID
			for key in worker_list[worker_addr]:
				if key == "tasks_done":
					continue
				
				if request_type == "maple_request":
					self.send_assignment(worker_addr, \
											request_type, \
											key, \
											exe, \
											worker_input=worker_list[worker_addr][key]["input"])
				else:
					self.send_assignment(worker_addr, \
											request_type, \
											key, \
											exe, \
											worker_input=None, \
											prefix=prefix, \
											keys=worker_list[worker_addr][key]["keys"])
		print("Post send assignment")

		# update request queue content
		self.request_q_lock.acquire()
		self.request_queue[0]["workers"] = worker_list
		self.request_queue[0]["workers_done"] = 0
		
		if request_type == "juice_request":
			self.request_queue[0]["keys"] = keys
		self.request_q_lock.release()

		return

	def check_n_reassign(self):
		'''
		Check if any of the workers assigned tasks has died, reassign if that is the case.
		Returns nothing.
		'''
		mem_list = self.get_memlist_in_addr()

		# find dead workers
		mem_decre = list(set(self.old_mem_list) - set(mem_list))

		self.request_q_lock.acquire()
		request_type = self.request_queue[0]["type"]
		if request_type == "maple_request":
			exe = self.request_queue[0]["maple_exe"]
		else:
			exe = self.request_queue[0]["juice_exe"]
			prefix = self.request_queue[0]["prefix"]
		
		for dead_worker_addr in mem_decre:
			if dead_worker_addr in self.request_queue[0]["workers"]:
				# Need reassigning assign to a free worker if exist or any alive worker if else
				# keep on trying until success
				task_dict = self.request_queue[0]["workers"][dead_worker_addr]

				reassign_done_flag = False
				while not reassign_done_flag:
					for alive_addr in mem_list:
						if not alive_addr in self.request_queue[0]["workers"]:
							# found free worker, assign tasks
							if request_type == "maple_request":
								if self.send_ls_assignment(alive_addr, \
															request_type, \
															exe, \
															task_dict):
									continue
							elif request_type == "juice_request":
								if self.send_ls_assignment(alive_addr, \
															request_type, \
															exe, \
															task_dict, \
															prefix=prefix):
									continue
							else:
								raise ValueError("Invalid request_type")

							# assigning done, update state
							self.request_queue[0]["workers"][alive_addr] = self.request_queue[0]["workers"][dead_worker_addr]
							del self.request_queue[0]["workers"][dead_worker_addr]
							reassign_done_flag = True
							break
					
					if not reassign_done_flag:
						# no free worker found, assign to existing worker
						for alive_addr in mem_list:
							if alive_addr in self.request_queue[0]["workers"]:
								# found org worker, assign tasks
								if request_type == "maple_request":
									if self.send_ls_assignment(alive_addr, \
																request_type, \
																exe, \
																task_dict):
										continue
								elif request_type == "juice_request":
									if self.send_ls_assignment(alive_addr, \
																request_type, \
																exe, \
																task_dict, \
																prefix=prefix):
										continue
								else:
									raise ValueError("Invalid request_type")

								# assigning done, combine state
								if self.request_queue[0]["workers"][dead_worker_addr]["tasks_done"] == len(self.request_queue[0]["workers"][dead_worker_addr]) - 1 or \
									self.request_queue[0]["workers"][alive_addr]["tasks_done"] == len(self.request_queue[0]["workers"][alive_addr]) - 1:
									self.request_queue[0]["workers_done"] -= 1

								self.request_queue[0]["workers"][dead_worker_addr]["tasks_done"] += self.request_queue[0]["workers"][alive_addr]["tasks_done"]
								self.request_queue[0]["workers"][alive_addr].update(self.request_queue[0]["workers"][dead_worker_addr])
								del self.request_queue[0]["workers"][dead_worker_addr]
								reassign_done_flag = True
								break

					if not reassign_done_flag:
						# all alive worker contact failed, update membership list and try again
						time.sleep(self.retry_assign_wait)
						mem_list = self.get_memlist_in_addr()

		self.request_q_lock.release()

		self.old_mem_list = mem_list

		return

	def daemon(self):
		'''
		Main class operation thread, started by UI thread.
		Returns nothing.
		'''

		welcome_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		welcome_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		welcome_sock.bind(("0.0.0.0", self.mj_port))
		welcome_sock.listen()
		welcome_sock.settimeout(self.listen_timeout)

		while True:
			print("Alive")
			try:
				conn, addr = welcome_sock.accept()
				print("Accepted")
				# received connection
				msg = recv_json_from_sock_block(conn, 4096)
				if not msg is None and len(msg) != 0:
					self.serve_msg(json.loads(msg), conn, addr[0])
				
				conn.close()
			except socket.timeout:
				pass # other exception should not happen
			except Exception as e:
				print(e)
				exit(1)

			self.request_q_lock.acquire()
			request_len = len(self.request_queue)
			self.request_q_lock.release()

			if not self.serving and request_len > 0:
				self.serve_queued_request()

			if self.serving:
				self.check_n_reassign()
			else:
				self.old_mem_list = self.get_memlist_in_addr()

		return

	def run(self):
		'''
		Function to start class operation, called directly.
		Returns nothing.
		'''

		self.t_fd = threading.Thread(target=self.fd.failure_detection, daemon=True)
		self.t_d = threading.Thread(target=self.daemon, daemon=True) # created but not started yet
		self.t_ui = threading.Thread(target=self.UI) # Thread ends signals all process to stop

		self.t_fd.start()
		self.fd_cmd_queue.put("introducer") # start master failure detector
		time.sleep(self.fd_start_wait)

		self.t_ui.start()

		self.old_mem_list = self.get_memlist_in_addr()

		return

	def UI(self):
		'''
		Main UI thread. Accepts commands and start daemon thread.
		Returns nothing.
		'''

		print("Hi, this is the MapleJuice master server.")
		while True:
			print("Available commands:")
			print("\t0: start master")
			print("\t1: show alive workers")
			print("\t2: show current requests")
			print("\t3: show membership ID")
			print("\t4: exit")

			cmd = input("Enter your command choice by specifying number: ")
			if cmd == "0":
				if self.t_d.ident is None:
					print("Starting the master daemon")
					self.t_d.start()
				else:
					print("Master daemon already started")					
			
			elif cmd == "1":
				#mem_list = self.get_memlist_in_addr()
				mem_list = self.old_mem_list
				print("Showing list of alive workers")
				for i in mem_list:
					print(i)
			
			elif cmd == "2":
				print("Showing current requests")
				self.request_q_lock.acquire()
				for req in self.request_queue:
					if req["type"] == "maple_request":
						print("Request type:\t{}, worker_num:\t{}, file_list:\t{}, prefix:\t{}".format(req["type"], \
																										req["worker_num"], \
																										req["file_list"], \
																										req["prefix"]))
					elif req["type"] == "juice_request":
						print("Request type:\t{}, worker_num:\t{}, prefix:\t{}, dest:\t{}, del_in:\t{}".format(req["type"], \
																												req["worker_num"], \
																												req["prefix"], \
																												req["dest_filename"], \
																												req["delete_input"]))
					else:
						print("ERROR ENCOUNTERED UNKNOWN REQUEST")
						print(req)
						exit(1)
				self.request_q_lock.release()

			elif cmd == "3":
				print("Self ID: {}".format(self.fd.ret_self_id()))

			elif cmd == "4":
				print("Arrivederci!")
				break

			else:
				print("Invalid command option, try again")
				continue

		return


if __name__ == "__main__":

	if len(sys.argv) < 2:
		print("Usage: python3 {} SHUFFLE_TYPE".format(sys.argv[0]))
		print("SHUFFLE_TYPE:")
		print("\t0: Hash partitioning")
		print("\t1: Range partitioning")
		exit(1)

	master = MapleJuiceMaster(2000, 2001, 2003, 2004, 2005, sys.argv[1])
	#master = MapleJuiceMaster(5000, 5001, 5003, 5004, 5005, sys.argv[1])
	master.run()
