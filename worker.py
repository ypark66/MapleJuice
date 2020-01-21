#!/usr/bin/python3
import sys
import time
import json
import base64
import queue
import socket
import threading
import collections
from failure_detector import RingFailureDetector
import tqdm
import os

'''
1. Assign Maple tasks to worker
    - "type"		: "assign_maple"
    - "task_ID"		: (integer: Non-repitive task ID)
    - "maple_exe" 	: (string: base64 encoded maple function executable)
    - "input"   	: (string: list of base64 encoded lines as input data)

2. Assign Juice tasks to worker
    - "type"		: "assign_juice"
    - "task_ID"		: (integer: Non-repitive task ID)
    - "juice_exe"	: (string: base64 encoded juice function executable)
    - "prefix"		: (string: sdfs intermediate file name prefix)
    - "keys"		: (list of strings: string being keys)

3. Acking returned results of the worker
    - "type"			: "ack"
    - "response_code"	: 0 or 1
'''
SDFS_SLAVE_PORT = 2001
SDFS_MASTER_PORT = 2000

class Worker:
    def __init__(self):
        self.master = "fa19-cs425-g43-01.cs.illinois.edu"
        self.master_port = 2003
        self.worker_dir = os.getcwd() + "/worker/"
        self.listen_timeout = 1
        self.fd_cmd_queue = queue.Queue()
        self.fd = RingFailureDetector(2004, self.fd_cmd_queue)
        self.t_fd = threading.Thread(target=self.fd.failure_detection, daemon=True)
        self.t_fd.start()
        self.fd_cmd_queue.put("join")
        self.task_queue = queue.Queue()
        self.queue_lock = threading.Lock()
        self.task_lock = threading.Lock()
        time.sleep(0.1)
        self.hb = self.fd.hb_on

    def run(self):
        task_thread = threading.Thread(target=self.serve_request, daemon=True)
        task_thread.start()

        while True:
            welcome_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            welcome_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            welcome_sock.bind(("0.0.0.0", self.master_port))
            welcome_sock.listen()
            welcome_sock.settimeout(self.listen_timeout)

            while True:
                try:
                    conn, _ = welcome_sock.accept()
                    if not self.hb:
                        print("Connection received, but false positive. Sending failure ack to master")
                        ack = {"type": "ack", "response_code": 1}
                        conn.sendall(json.dumps(ack).encode())
                        conn.close()
                        break
                    data = json.loads(self.recv_json_from_sock_block(conn, 4096))
                    data["time"] = time.time()

                    print("Worker received request")
                    self.queue_lock.acquire()
                    self.task_queue.put(data)
                    print("Queue added. length of queue is {0}".format(self.task_queue.not_empty))
                    self.queue_lock.release()

                    ack = {"type": "ack", "response_code": 0}
                    conn.sendall(json.dumps(ack).encode())
                    print("sent ack to master after receiving request")
                    conn.close()

                except socket.timeout as e:
                    if not self.hb:
                        print("Dead worker. (timeout)")
                        break

            if not self.hb:
                print("Dead worker.")
                welcome_sock.close()
                break

    def serve_request(self):
        # cmd_msg    : command message sent by client or master
        # conn        : client tcp connection
        while True:
            self.queue_lock.acquire()
            if not self.task_queue.empty():
                cmd_msg = self.task_queue.get()
                print("received request {0}".format(cmd_msg["type"]))
                self.queue_lock.release()
                with open("worker_log.log", "a") as fp:
                    if cmd_msg["type"] == "assign_maple":
                        print("maple requested")
                        fp.write("{}\tget_replicate command received. msg_content: {}\n".format(time.time(), cmd_msg))
                        self.slave_assign_maple(cmd_msg)
                        print("maple finished")
                    elif cmd_msg["type"] == "assign_juice":
                        print("juice requested")
                        fp.write("{}\tget_replicate command received. msg_content: {}\n".format(time.time(), cmd_msg))
                        self.slave_assign_juice(cmd_msg)
                        print("juice finished")
                    else:
                        print("invalid type received")
            else:
                self.queue_lock.release()
                time.sleep(0.01)



    def slave_assign_maple(self, cmd):
        exe = cmd['maple_exe']
        task_id = str(cmd['task_ID'])
        input = cmd['input']
        print("assign_maple")

        #write exe to import
        package = "maple" + task_id
        with open(os.path.join(os.getcwd(), package +".py"), "w") as fp:
            fp.write(exe)

        result = collections.defaultdict(list)
        maple = getattr(__import__(package, fromlist=["maple"]), "maple")

        l = len(input)
        print("The length of input is {0}".format(l))
        i = 0
        if l > 0:
            flag = 1
            while flag:
                data = ""
                for _ in range(10):
                    data += input[i] + '\n'
                    i+=1
                    if i == l:
                        flag = 0
                        break
                # print(data)
                # print(len(data))
                output = maple(data)
                # print(output)
                for k, v in output.items():
                    result[k] += [v]
                # if i > 15:
                #     break

        print("number of lines served : {0}".format(i))
        print("Served keys : {0}".format(result.keys()))
        msg = {'type': 'result', 'task_ID': int(task_id), 'rst': result}
        print("done")

        if self.hb:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                #sock.settimeout(1)
                try :
                    sock.connect((self.master, self.master_port))
                except:
                    print("Could not connect to master. Not sending the result")
                    return
                while 1:
                    try:
                        print("sending result")
                        sock.sendall(json.dumps(msg).encode())
                        ack = sock.recv(1024)
                        msg = json.loads(ack)
                        print("Received msg from master : {0}".format(msg))
                        sock.close()
                        return
                    except Exception as e:
                        print(e)
                        print("sending to master failed. doing again")

                        continue


    def slave_assign_juice(self, cmd):
        exe = cmd['juice_exe']
        task_id = str(cmd['task_ID'])
        prefix = cmd['prefix'] + "_"
        l_keys = cmd['keys']
        print("Worker got keys: {}".format(cmd['keys']))
        result = collections.defaultdict(list)

        # write exe to import
        package = "juice" + task_id
        with open(os.path.join(os.getcwd(), package + ".py"), "w") as fp:
            fp.write(exe)
        juice = getattr(__import__(package, fromlist=["juice"]), "juice")

        while l_keys:
            key = l_keys.pop()
            print("Juicing the key : {0}".format(key))
            self.juice_file_path = os.path.join(self.worker_dir, prefix + key)
            #self.request_get_file(prefix+key)
            while True:
                print("Getting file ({}) from SDFS".format(prefix+key))
                data = self.get_file_from_sdfs(prefix+key, SDFS_MASTER_PORT, SDFS_SLAVE_PORT)
                if not data is None:
                    break
                time.sleep(1)
            print("get file successful")
            
            output = juice(json.loads(data))
            for k, v in output.items():
                result[k] += [v]                

        print("Juice result is {0}".format(result))
        msg = {'type': 'result', 'task_ID': int(task_id), 'rst': result}

        if self.hb:
            while True:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    try :
                        sock.connect((self.master, self.master_port))
                    except sock.timeout as e:
                        print(e)
                        continue

                    try:
                        print("sending result")
                        sock.sendall(json.dumps(msg).encode())
                        ack = sock.recv(1024)
                        msg = json.loads(ack)
                        if msg["type"] == 'ack':
                            print("received ack")
                            sock.close()
                            return
                    except:
                        print("sending to master failed. doing again")
                        sock.close()
                        continue



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


    def request_get_file(self,f_name):

        packet = {'type': 'get_file', 'filename': f_name}

        # send request to master
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = (self.get_master(), SDFS_MASTER_PORT)   #TODO CHANGE PORT TO SDFS
        try:
            sock.connect(server_address)
        except:
            return '{"data":"Connection Failure"}'
        data = ""

        print('socket connect')
        try:
            msg = json.dumps(packet).encode()
            print('sending message {0}'.format(msg))
            sock.sendall(msg)
            data = sock.recv(1024)
            data = json.loads(data)
            data = data['response']  # ip
            print('got data {0} from master'.format(data))

        finally:
            sock.close()

        # receive file from the slave
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = (data, SDFS_SLAVE_PORT)                 #TODO CHANGE PORT TO SDFS
        try:
            sock.connect(server_address)
        except:
            return '{"data":"Connection Failure"}'
        data = ""

        print('socket connect')
        try:
            msg = json.dumps(packet).encode()
            print('sending message {0}'.format(msg))
            sock.sendall(msg)
            data = sock.recv(1024)
            data = json.loads(data)
            size = data['response']
            self.recv_file(f_name, size, "worker", sock)
        finally:
            sock.close()
        filesize = os.path.getsize("worker")
        print('Received {0}  size {1}'.format(f_name, filesize))
        return

    def recv_file(self, f_name, file_size, f_path, conn):
        with open(self.juice_file_path, "wb") as fp:  # fix
            # read chunk by chunk with tqdm to avoid memory exhaustion
            print("Receiving file: {} from client having size: {}".format(f_name, file_size))
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
                if recv_len == file_size:
                    break
        return 1 if recv_len == file_size else 0

    def get_master(self):
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
        packet = {'type': 'get_master'}
        while ip_list:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            #sock.settimeout(1)
            a = ip_list.pop()
            server_address = (a, SDFS_SLAVE_PORT)            #TODO CHANGE TO SDFS slave port 1001
            try:
                sock.connect(server_address)
            except:
                print('Connection failure to {0}'.format(a))
                continue
            data = ""

            print('socket connect')
            try:
                msg = json.dumps(packet).encode()
                print('sending message {0}'.format(msg))
                sock.sendall(msg)
                data = sock.recv(1024)
                data = json.loads(data)
                if data['response_code'] == 1:
                    print('Did not get master. Continue to next ip')
                    continue
                else:
                    master_ip = data['response'].split(' ')[0]
                    break
            except:
                print('Connection Failure with {0}'.format(a))
                sock.close()
                return None

            finally:
                sock.close()
        print('Master IP: {0}'.format(master_ip))
        return master_ip

    def recv_file_in_mem(self, filename, filesize, conn):
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

    def get_file_from_sdfs(self, sdfs_filename, sdfs_master_port, sdfs_slave_port):
        '''
        :Get file from sdfs.
        :sdfs_filename: string, filename of target file in sdfs system.
        :sdfs_master_port: integer, port that the sdfs master is listening on.
        :sdfs_slave_port: integer, port that the sdfs slave is listening on.
        :return either None or targte file content (bytes).
        '''

        sdfs_master_ip = self.get_master()
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
            rst = self.recv_file_in_mem(sdfs_filename, size, sock)
        except Exception as e:
            print(e)
        finally:
            sock.close()

        return rst

    def recv_json_from_sock_block(self, sock, block_size):
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

    def fd_failure(self):
        while True:
            if not self.fd.hb_on:
                self.hb = 0
                self.fd_cmd_queue.put('exit')
                break
            else:
                time.sleep(0.1)


if __name__ == "__main__":
    w = Worker()
    w.run()


