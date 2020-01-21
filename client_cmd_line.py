#!/usr/bin/python3
import socket
import sys
import threading
import time
import random
import argparse
import json
import os
import tqdm
import sys
import select

ip_listt = ['fa19-cs425-g43-01.cs.illinois.edu',
           'fa19-cs425-g43-02.cs.illinois.edu',
           'fa19-cs425-g43-03.cs.illinois.edu',
           'fa19-cs425-g43-04.cs.illinois.edu',
           'fa19-cs425-g43-05.cs.illinois.edu',
           'fa19-cs425-g43-06.cs.illinois.edu',
           'fa19-cs425-g43-07.cs.illinois.edu',
           'fa19-cs425-g43-08.cs.illinois.edu',
           'fa19-cs425-g43-09.cs.illinois.edu',
           'fa19-cs425-g43-10.cs.illinois.edu']
PORT = 2000
SLAVE_PORT = 2001

def fetch_data(ip, command):
    '''
    Connect to the server
    send the request packet
    wait for the respones
    close the connection
    return the response
    '''
    # logging = logger(ip,'log/{0}.log'.format(ip)).logger
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    server_address = (ip, 2156)

    try:
        sock.connect(server_address)
    except:
        return '{"data":"Connection Failure"}'
    data = ""

    try:
        message = str(command)
        sock.sendall(message.encode('UTF-8'))

        data = sock.recv(104857600)
        # print('got data {0}'.format(data))

    finally:
        sock.close()

    if data != "":
        return data.decode('UTF-8')
    else:
        return '{"data":"Connection Failure"}'


def connection_thread(ip, command, thread_list, idx):
    ret_data = fetch_data(ip, command)
    thread_list[idx][1] = ret_data


def start_connection(ip, command, thread_list, idx):
    '''
    This function spins up a thread for the passed ip
    and command and returns the output from the command
    '''
    t = threading.Thread(target=connection_thread, args=(ip, command, thread_list, idx,))
    t.start()
    return t


def poll_machines(packet, vm_num=-1):
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

    thread_list = []
    if vm_num == -1:  # if vm_num is -1, add all vms to thread
        for a in range(len(ip_list)):  # spawning the threads
            thread_list.append([start_connection(ip_list[a], packet, thread_list, a), "", ip_list[a]])
    else:
        thread_list.append([start_connection(ip_list[vm_num - 1], packet, thread_list, 0), "", ip_list[vm_num - 1]])
    data_list = []  # what is returned

    for thread in thread_list:
        thread[0].join()
        data_list.append((thread[2], thread[1]))  # ip of the machine and the returned data

    return data_list


def request_grep(grep_args):
    packet = {'type': 'grep',
              'args': grep_args}
    data_list = poll_machines(json.dumps(packet))
    for data in data_list:
        print(json.loads(data[1])['data'])


def request_make_test(arg):
    packet = {'type': 'gen_test_files',  # packet type
              'line_text': str(arg[0]),  # the good line to put in with the other randoms
              'random_count': int(arg[1]),  # number of random character lines
              'good_count': int(arg[2]),  # the count of good lines
              'file_name': str(arg[3])}  # the name of the file

    poll_machines(json.dumps(packet))


def request_log_query(log_args):
    packet = {'type': 'log',
              'args': log_args}
    data_list = poll_machines(json.dumps(packet), vm_num=int(log_args[2]))
    for data in data_list:
        print(json.loads(data[1])['data'])


def request_halt(arg):
    # halt all service

    if not arg:
        packet = {'type': 'halt'}
        data_list = poll_machines(json.dumps(packet))
        for data in data_list:
            print(json.loads(data[1])['data'])
    # halt specified service
    else:
        packet = {'type': 'halt'}
        data_list = poll_machines(json.dumps(packet), vm_num=int(arg[0]))
        for data in data_list:
            print(json.loads(data[1])['data'])


def request_put_file(args):
    f_name = os.getcwd()+ "/"+args[0]
    sdfs_f_name = args[1]
    print('Requesting put_file of file name {0} with sdfs name {1}'.format(f_name, sdfs_f_name))

    f_path = os.path.join('file', f_name)
    filesize = os.path.getsize(f_path)
    packet = {'type': 'put_file',
              'size': filesize, 'filename': sdfs_f_name}
    print('file path: {0}'.format(f_path))
    print('file size : {0}'.format(filesize))

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = (get_master(), PORT)
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
        print("sent")
        data = sock.recv(1024)
        print(data)

        # the server may notify 60 sec write, deal with it
        data = json.loads(data.decode())
        if data["type"] == 1:
            # notified of 60 s write, ask user
            consent_ack = {}
            prompt_time = time.time()
            while True:
                if time.time() > 30 + prompt_time:
                    print("30 second is up")
                    print("Abort put file")
                    consent_ack["type"] = 0
                    sock.sendall(json.dumps(consent_ack).encode())
                    data = sock.recv(1024)
                    print("Ack: {}".format(data))
                    sock.close()
                    return data.decode()

                print("The target sdfs file: {}, was modified previously within 60 seconds".format(sdfs_f_name))
                print("Please provide explicit consent to proceed (y/n)")
                i, _, _ = select.select( [sys.stdin], [], [],  max(0, (30 + prompt_time - time.time())))
                if len(i) != 0:
                    consent_rst = sys.stdin.readline().strip()
                else:
                    continue

                if consent_rst == "y":
                    print("Continue on putting file")
                    consent_ack["type"] = 1
                    sock.sendall(json.dumps(consent_ack).encode())
                    break
                elif consent_rst == "n":
                    print("Abort put file")
                    consent_ack["type"] = 0
                    sock.sendall(json.dumps(consent_ack).encode())
                    data = sock.recv(1024)
                    print("Ack: {}".format(data))
                    sock.close()
                    return data.decode()
                else:
                    print("Invalid answer")
                
        send_file(f_name, f_path, sock)
        data = sock.recv(1024)
        # print('got data {0}'.format(data))

    finally:
        sock.close()

    if data != "":
        return data.decode('UTF-8')
    else:
        return '{"data":"Connection Failure"}'


def request_get_file(args):
    f_name = args[0]
    print('Requesting get_file with sdfs name {0}'.format(f_name))

    f_path = os.path.join('client', args[1])
    packet = {'type': 'get_file', 'filename': f_name}
    print('file path: {0}'.format(f_path))

    # send request to master
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = (get_master(), PORT)
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
    server_address = (data, SLAVE_PORT)
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
        recv_file(f_name, size, f_path, sock)
    finally:
        sock.close()
    filesize = os.path.getsize(f_path)
    print('Received {0}  size {1}'.format(f_name, filesize))
    return

def get_master():
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
    packet = {'type':'get_master'}
    while ip_list:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        a = ip_list.pop()
        server_address = (a, SLAVE_PORT)
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

        finally:
            sock.close()
    print('Master IP: {0}'.format(master_ip))
    return master_ip

def recv_file(f_name, file_size, f_path, conn):
    with open(f_path, "wb") as fp:  # fix
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


def send_file(f_name, f_path, conn):
    filesize = os.path.getsize(f_path)
    # ack = {"type": "ack"}
    # ack["response_code"] = 0
    # ack["response"] = filesize
    # conn.sendall(json.dumps(ack).encode())
    print('send file')
    with open(f_path, "rb") as fp:
        # send chunk by chunk with tqdm to avoid memory exhaustion
        print("Sending file: {} to client having size: {}".format(f_name, filesize))
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


def request_del_file(args):
    f_name = args[0]
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(50)
    try:
        request = {"type": "del_file", 'filename': f_name}
        sock.connect((get_master(), PORT))
        print('connect')

        sock.sendall(json.dumps(request).encode())  # timeout applies, but should not do harm
        data = sock.recv(1024)
        data = json.loads(data)
        print(data)
        sock.close()
    finally:
        return


def request_ls_file(args):
    f_name = args[0]
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(50)
    try:
        request = {"type": "ret_file_ls", 'filename': f_name}
        sock.connect((get_master(), PORT))
        print('connect')
        sock.sendall(json.dumps(request).encode())  # timeout applies, but should not do harm
        data = sock.recv(1024)
        data = json.loads(data)
        if data['response_code'] == 0:
            ip_list = data['response']['vm']
            sorted(ip_list)
            print(ip_list)
        else:
            print(data)
        sock.close()
    finally:
        return

def request_store_file(arg):
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
    packet = {'type':'ret_file_ls'}
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1)
    ip =ip_list[int(arg[0])-1]
    server_address = (ip , SLAVE_PORT)
    try:
        sock.connect(server_address)
    except:
        print('Connection failure to {0}'.format(ip))
        return
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
        else:
            print(data['response'])
    except:
        print('Connection Failure with {0}'.format(a))
    finally:
        sock.close()

    return

def parse_args():
    description = '''
    Used to grep on files across multiple machines\n
    * grep (use the tool)
    * make_test (create files on all the machines)
    '''
    while True:
        print("Type in the command: ")
        x = input()
        args = x.split(' ')
        unknown = args[1:]
        args = args[0]
        #stime = time.time()
        if args == 'grep':
            request_grep(unknown)
        elif args == 'make_test':
            request_make_test(unknown)
        elif args == 'log':
            request_log_query(unknown)
        elif args == 'halt':
            request_halt(unknown)
        elif args == 'put':
            request_put_file(unknown)
        elif args == 'get':
            request_get_file(unknown)
        elif args == 'del':
            request_del_file(unknown)
        elif args == 'ls':
            request_ls_file(unknown)
        elif args == 'store':
            request_store_file(unknown)
        elif args == 'get_master':
            get_master()
        else:
            print("unrecognized command try running with --help for a list of commands")
        #print("FIN time: {}".format(time.time() - stime))


if __name__ == '__main__':
    # logging = logger('client','log/client.log').logger
    parse_args()








