import time
import random
import json
import queue
import socket
import threading

from logger import logger
from DoublyLinkedList import DoublyLinkedList

"""
How a node joining the system works
    A node will join right next to the introducer. The node next to the introducer is old_node
    new node requests to join.
    Introducer sends joining request with the ip address of the newly joining node, new_node to the old_node.
    old_node sends to the new node : hb_send_table, and hb_recv_table including ip of old_node.
    old_node sends to every ip address in hb_recv_table : new_node ip with index to change.
    old_node sends to every ip address in hb_send_table new_node ip with index to change.
    new_node receives hb_send_table and hb_recv_table and updates the tables. Start sending heartbeat.

 TODO: find out efficient way to update the hb_send_table and hb_recv_table. How to manipulate elements
 TODO: find out how joining and leaving works when there are less then 3 nodes in the system.

How a node leaving the system works
    old node sends leave request to introducer.
    Receive ACK from introducer, with the ip address of the predecessor. The predecessor = prev_node
    old node sends to the prev_node : hb_send_table

    fill out

"""

# TODO: listening thread no getting turned off sometimes
# TODO: change name of the key according to the joining time
# TODO: UDP State tracking
# TODO: change the name of n_size to max_failure_number

class RingFailureDetector:
    def __init__(self, fd_port, fd_cmd_queue):
        self.hb_recv_table = {}                                           #key : IP address / Value : [Counter, local time]
        self.hb_recv_table_lock = threading.Lock()
        self.hb_send_table = {}                                          #key : IP address / Value : Counter
        self.hb_send_table_lock = threading.Lock()
        self.introducer = 0                                                  # 1 if the node is introducer else 0
        self.active_node_list = DoublyLinkedList()           #linked list of all nodes in the system for introducer
        self.active_node_list_lock = threading.Lock()      #implement this later. Conflict happens only when leave and join happens at the same time
        self.active_node_list_size = 0
        self.self_node = 0                        #pointer to the node object of self
        self.introducer_list = []
        self.failed_server_list = set()       #set of names of server declared failed
        self.failure_period = 5                 #period of time to declare a server failed
        self.hb_period = 1                       #period of sending heartbeat
        self.hb_on = 0
        self.n_size = 3  # number of maximum failure allowed for completeness of the system
        self.log = logger('failure_detection', 'log/failure_detector.log').logger
        self.prim_intro_addr = 'fa19-cs425-g43-01.cs.illinois.edu' # for normal nodes to join
        self.sec_intro_addr = 'fa19-cs425-g43-02.cs.illinois.edu' # for introducer to rejoin
        self.fd_port = fd_port
        self.cmd_queue = fd_cmd_queue
        self.rejoin = 0 # for false positive rejoining

    def init_introducer(self):
        '''
        Initialization of the introducer server
        :return: void
        '''
        self.introducer = 1
        (host_name, _) = self.get_host_name_ip()
        self.introducer_list.append(host_name)
        node = self.active_node_list.insert_in_emptylist(host_name)
        new_id = str(host_name) + " " + str(time.ctime())
        node.id = new_id
        self.self_node = node
        self.active_node_list_size = 1

    def join_new_node(self, load, address):
        '''
        Request given from new server to the introducer. Adds the new server to the system.
        Updates membership list of all servers in the system and updates own sender receiver table.
        :param load: type, join new node args, new server ip address
        :param address: unused
        :return: void
        '''

        new_ip = load['args']
        new_id = str(new_ip) + " " + str(time.ctime())                          #id with the time stamp

        self.active_node_list_lock.acquire()
        first_node = self.active_node_list.start_node

        old_node, new_node = first_node.nref, self.active_node_list.insert_after_node(first_node, new_ip)     #add the new node in the system
        new_node.id = new_id
        self.active_node_list_size += 1
        if old_node == None:         #set up ring topology
            new_node.nref = first_node
            first_node.pref = new_node

        self.log.info('join_new_node New node added to active node list')  # consider case when new node fails to join, and have to delete this node from the list

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

            #traverse all active nodes and request to add a new node to their active_node_list
            target_node = new_node
            self_hostname, _ = self.get_host_name_ip()
            while target_node.nref != new_node:  #update active_node_list to all old nodes of system
                target_node = target_node.nref
                
                # no need to send to self (i.e. the introducer)
                if target_node.item == self_hostname:
                    continue

                packet= {'type': 'add_new_node', 'args': (new_node.item, new_node.id)}
                request = json.dumps(packet)
                
                server_address = (target_node.item, self.fd_port)
                sock.sendto(request.encode(), server_address)  # send data
                self.log.info('join_new_node add_new_node request to {0}'.format(target_node.item))

            #send active_node_list to new node (new node sets up its sender and receiver table)
            active_list = self.active_node_list.return_list_with_id()
            server_address = (new_node.item, self.fd_port)
            
            self.active_node_list_lock.release()

            packet = {'type':'copy_active_list', 'args':active_list}
            request = json.dumps(packet)
            
            sock.sendto(request.encode(), server_address)  # send data
            self.log.info('join_new_node Sent active membership list to {0}'.format(server_address[0]))

        #after updating all active membership list in system, update tables
        self.update_tables()

    def request_join(self, introducer_ip):
        '''
        Function that runs when command Join was given from the command prompt
        Requests join to introducer of given ip to join the system.
        :param introducer_ip: ip to the introducer
        :return: void
        '''
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            self.log.info('Request join to introducer initiated')

            packet = {'type':'join', 'args':socket.gethostname()}
            request = json.dumps(packet)

            server_address = (introducer_ip, self.fd_port)
            sock.sendto(request.encode(),server_address)

            self.log.info('Sent join request to {0}'.format(introducer_ip))
        
        return

    def request_leave(self):
        '''
        Request leaving from the system. Updates all server's membership list
        :return: void
        '''
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            self.log.info('Request leave initiated')

            self.active_node_list_lock.acquire()

            target_node = self.active_node_list.start_node
            for _ in range(self.active_node_list_size):  #update active_node_list to all old nodes of system
                if target_node == self.self_node:
                    target_node = target_node.nref
                    continue

                packet = {'type': 'delete_node', 'args': (socket.gethostname())}
                request = json.dumps(packet)

                server_address = (target_node.item, self.fd_port)
                sock.sendto(request.encode(), server_address)  # send data

                self.log.info('request_leave leave_node request to {0}'.format(target_node.item))
                target_node = target_node.nref

            self.active_node_list_lock.release()

        return

    def heartbeat_sender(self):
        '''
        Runs in background and sends heartbeat to all ip in hb_sender_table every self.hb_period.
        At each heartbeat the count of the heartbeat gets incremented
        :return: None
        '''
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.log.info('heartbeat_sender Heartbeat initiated')
            while self.hb_on:

                self.log.info('heartbeat_sender Acquiring hb_send_table_lock')
                self.hb_send_table_lock.acquire()

                self.update_hb_count()           # update counter
                for key in self.hb_send_table:   # send heartbeat to all members in membership table
                    packet = {'type':'heartbeat','args': (self.hb_send_table[key], socket.gethostname())}        #args = counter value
                    hb = json.dumps(packet)

                    server_address = (key, self.fd_port)
                    sock.sendto(hb.encode(),server_address)
                    
                    self.log.info('heartbeat_sender Sending Heartbeat to {0}, count: {1}'.format(key, key[0]))

                self.hb_send_table_lock.release()
                self.log.info('heartbeat_sender Releasing hb_send_table_lock')

                self.log.info('heartbeat_sender Wait...')
                time.sleep(self.hb_period)                     # wait until next heartbeat

        print('heartbeat sender off')
        self.log.warning('heartbeat_sender Heartbeat Terminating')

    def heartbeat_listener(self):
        '''
        function that listens and handles messages sent from failure detector of other active servers in the system.
        :return:
        '''
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.log.info('heartbeat_listener Heartbeat Listen Initiated')
            server_address = ('0.0.0.0', self.fd_port)
            #s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(server_address)
            s.settimeout(1)
            while self.hb_on:
                try:
                    data, address = s.recvfrom(4096)
                except socket.timeout:
                    # for periodical checking of self.hb_on
                    continue

                load = json.loads(data.decode('UTF-8'))
                self.log.info('heartbeat_listener Received {0} request from {1}'.format(load['type'],address))
                
                if load['type'] == 'heartbeat':
                    self.log.info('heartbeat_listener Received heartbeat from {0}'.format(address))
                    self.heartbeat_handler(load, address)

                elif load['type'] == 'join': #request sent only to the introducer #update sender receiver (introducer) (increment size)
                    print('Join {0}'.format(address))
                    self.log.info('heartbeat_listener Received join request from {0}'.format(address))
                    self.join_new_node(load, address)

                elif load['type'] == 'add_new_node':                                                                   #update sender receiver (all active nodes in the system) (increment size)
                    print('Adding {0}'.format(address))
                    self.add_new_node(load, address)

                elif load['type'] == 'failure_alert':
                    failed_ip = load['args']
                    print('--------->Got failure alert {} from {} <---------'.format(failed_ip, address))
                    
                    # false positive
                    if socket.gethostname() == failed_ip:
                        # TB: should leave and rejoin
                        print("False positive detected, leaving and rejoining")
                        self.hb_on = 0
                        self.rejoin = 1

                    # self.failed_server_list.add(failed_ip)
                    # direct remove IP if exist
                    load["type"] = 'delete_node'
                    self.delete_node(load, address) # does checking before delete
                    
                elif load['type'] == 'copy_active_list':                                                                 #update sender receiver (new node) get new size
                    print('copy_active_list')
                    self.copy_active_list(load, address)

                elif load['type'] == 'delete_node':
                    print('delete node')
                    self.delete_node(load, address)

                else:
                    self.log.error('heartbeat_listener GOT INVALID MESSAGE')
            
            s.close()

        print('heartbeat listener off')
        self.log.warning('heartbeat_listener heartbeat_listener thread off')

    def failure_checker(self):
        '''
        function that runs on background to check for failures. Calculates the difference between current time and the last time
        the server got heartbeat message from certain nodes. When detects a failure, sends failure alert to all active servers in the system.
        :return: None
        '''
        while self.hb_on:
            #stop checking for failure of the nodes that has been acknowledged as failed

            failed_ls = []

            self.log.info('failure_checker Acquiring hb_receive_table_lock')
            self.hb_recv_table_lock.acquire()

            for key in self.hb_recv_table:
                if key in self.failed_server_list:
                    continue

                _, last_time = self.hb_recv_table[key]
                local_time = time.time()
                if int(local_time - last_time) > self.failure_period:
                    failed_ls.append(key)
                    
            self.log.info('failure_checker Releasing hb_receive_table_lock')
            self.hb_recv_table_lock.release()

            # remove all failed
            for key in failed_ls:
                print('----->Server {} has failed<-----'.format(key))
                # first notify all node in original list, to prevent false positive 
                self.send_failure_alert(key)
                
                # self.failed_server_list.add(key)
                # rather than adding to failure list, direct remove from list
                load = {'type':'delete_node','args':key}
                hostname, _ = self.get_host_name_ip()
                self.delete_node(load, hostname)

            time.sleep(1)

        print('failure_checker failure checker off')
        self.log.warning('failure_checker thread off')

    def send_failure_alert(self,failed_ip):
        '''
        sends ip address of failed server to all active nodes in the system
        :param failed_ip: ip address of failed server
        :return: None
        '''
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

            self.active_node_list_lock.acquire()
            target_node = self.active_node_list.start_node
            for _ in range(self.active_node_list_size):  # update active_node_list to all old nodes of system
            
                if target_node.item == socket.gethostname():
                    target_node = target_node.nref
                    continue
            
                packet = {'type': 'failure_alert', 'args': failed_ip}
                request = json.dumps(packet)

                server_address = (target_node.item, self.fd_port)
                sock.sendto(request.encode(), server_address)  # send data
                target_node = target_node.nref
            
            self.active_node_list_lock.release()

    def failure_detection(self):
        '''
        Main function that runs failure detection. Prompts user and runs functions according to the given command
        :return: void
        '''
        on = 1

        # start UI thread, use queue to pass command between UI and main thread
        # if want to implement new UI, just don't start the UI thread and pass directly with queue
        
        while on:

            # read from queue to get command, timeout every once in a while to check if rejoin
            try:
                cmd = self.cmd_queue.get(timeout=1)
                self.log.info('failure_detection Typed cmd: {0}'.format(cmd))
            except queue.Empty:
                # timed out
                if self.rejoin == 1:
                    # got rejoin flag, sleep and rejoin
                    print("Processing rejoin")
                    self.rejoin = 0
                    time.sleep(10)
                    cmd = "join"
                else:
                    # no rejoin flag, just continue
                    continue
            
            if cmd == "introducer":
                self.init_introducer()

                if self.hb_on == 0:
                    self.hb_on = 1
                    t_s = threading.Thread(target=self.heartbeat_sender, daemon=True)
                    t_s.start()
                    self.log.info('failure_detection Heartbeat Sender initiated')

                    t_l = threading.Thread(target=self.heartbeat_listener, daemon=True)
                    t_l.start()
                    self.log.info('failure_detection Heartbeat listener initiated')

                    t_f = threading.Thread(target=self.failure_checker, daemon=True)
                    t_f.start()
                    self.log.info('failure_detection Failure checker initiated initiated')
                else:
                    self.log.info('failure_detection heartbeat already on')
                    print('Heartbeat already on')

            elif cmd == 'join':
                if self.hb_on == 0:
                    # reinit after each join
                    self.__init__(self.fd_port, self.cmd_queue)

                    self.hb_on = 1
                    t_l = threading.Thread(target=self.heartbeat_listener, daemon=True)   #start listener first to get data from introducer
                    t_l.start()
                    self.log.info('failure_detection Heartbeat listener initiated')

                    t_s = threading.Thread(target=self.heartbeat_sender, daemon=True)
                    t_s.start()
                    self.log.info('failure_detection Heartbeat Sender initiated')

                    t_f = threading.Thread(target=self.failure_checker, daemon=True)
                    t_f.start()
                    self.log.info('failure_detection Failure checker initiated initiated')

                else:
                    self.log.info('Heartbeat already listener initiated')
                    print('listening already initiated')

                # deal with introducer rejoin special case by having 
                # secondary introducer on vm02. is used when process on vm01 wants to join
                (host_name, _)= self.get_host_name_ip()
                target_intro = self.prim_intro_addr
                if host_name == target_intro:
                    target_intro = self.sec_intro_addr

                self.request_join(target_intro)

            elif cmd == 'leave':
                # when introducer wants to leave? set other server as the introducer
                self.request_leave()
                print('Leaving')
                self.hb_on = 0
                #time.sleep(10)
                #on = 0

            elif cmd == 'membership list':
                self.active_node_list_lock.acquire()
                self.active_node_list.traverse_list()
                self.active_node_list_lock.release()
            
            elif cmd == 'membership list id':
                self.active_node_list_lock.acquire()
                self.active_node_list.get_all_id()
                self.active_node_list_lock.release()
            
            elif cmd == 'self id':
                (host_name, host_ip)= self.get_host_name_ip()
                if host_name:
                    print("Hostname :  ", host_name)
                    print("IP : ", host_ip)

            elif cmd == 'get table':
                print('Heartbeat Receiving Table')
                print(self.hb_recv_table)
                print('Heartbeat Sending Table')
                print(self.hb_send_table)

            elif cmd == 'failure list':
                print(self.failed_server_list)

            elif cmd == 'reverse list':
                self.active_node_list_lock.acquire()
                node = self.active_node_list.start_node
                for _ in range(self.active_node_list_size):
                    print(node.item)
                    node = node.pref
                self.active_node_list_lock.release()

            elif cmd == "exit":
                print("The time has come, shutting down in 3 2 1")
                on = 0

        print('Quitting Failure Detection')

    def heartbeat_handler(self, load, address):
        '''
        receives heartbeat from a node and updates hb_recv_table
        :param load: {'type':'heartbeat', 'args':(counter, ip_address)}
        :param address: ip of the heartbeat sender
        :return: void
        '''
        count, hb_ip = load['args']

        self.log.info('heartbeat_handler Acquiring hb_receive_table_lock  {0}'.format(hb_ip))
        self.hb_recv_table_lock.acquire()
        if hb_ip not in self.hb_recv_table:
            self.hb_recv_table[hb_ip] = (0,time.time())

            self.hb_recv_table_lock.release()
            self.log.info('heartbeat_handler Releasing hb_receive_table_lock')
            return

        if self.hb_recv_table[hb_ip][0] < count:
            self.hb_recv_table[hb_ip] = (count, time.time())

        self.hb_recv_table_lock.release()
        self.log.info('heartbeat_handler Releasing hb_receive_table_lock')
        return

    def handle_false_positive(self, false_ip):
        '''
        deletes an ip address of false positive from the failed server list
        :param false_ip: ip address of false positive
        :return:
        '''
        self.failed_server_list.remove(false_ip)

    def add_new_node(self,load, address):
        '''
        Adds a new active server in active membership list
        :param load: load['args'] contains the ip of the new node
        :param address: ip address of the introducer
        :return:
        '''
        new_ip, new_id = load['args']
        self.active_node_list_lock.acquire()
        node = self.active_node_list.insert_after_node(self.active_node_list.start_node, new_ip)
        node.id = new_id
        self.active_node_list_size += 1
        self.active_node_list_lock.release()
        self.update_tables()

    def delete_node(self,load, address):
        '''
        deletes a server of given ip address from membership list and update sender and receiver table
        :param load: {'type':'delete_node,'args':ip_address}
        :param address: ip address where the request was sent from
        :return: void
        '''
        update_flag = False
        del_ip = load['args']
        self.active_node_list_lock.acquire()
        
        # check before deleting
        if self.active_node_list.check_if_in(del_ip):
            self.active_node_list.delete_element_by_value(del_ip)
            self.active_node_list_size -= 1
            update_flag = True

        self.active_node_list_lock.release()
        
        if update_flag:
            self.update_tables()

    def copy_active_list(self,load, address):
        '''
        Function for new joining node, gets a list of active members from another node
        and adds it to doubly linked list of active membership list
        :param load: json packet with list of active members as value for key 'args'
        :param address: ip address of the sender
        :return: None
        '''
        list_active = load['args']
        self_ip = socket.gethostname()

        if list_active == None: return
        if len(list_active) == 1: return

        ip, id = list_active[0]

        self.active_node_list_lock.acquire()

        self.active_node_list.start_node = self.active_node_list.insert_in_emptylist(ip)
        self.active_node_list.start_node.id = id

        start = self.active_node_list.start_node
        if start.item == self_ip: self.self_node = start

        ip, id = list_active[1]
        new_node = self.active_node_list.insert_after_item(start.item, ip)
        new_node.id = id

        self.active_node_list.tail_node = new_node
        if new_node.item == self_ip: self.self_node = new_node

        self.active_node_list_size = 2
        for i in range(2,len(list_active)):
            ip, id = list_active[i]
            node = self.active_node_list.insert_at_end(ip)
            node.id = id
            if node.item == self_ip: self.self_node = node
            self.active_node_list_size += 1

        self.active_node_list.tail_node.nref = start
        start.pref = self.active_node_list.tail_node
        
        self.active_node_list_lock.release()

        self.update_tables()

    def update_tables(self):
        '''
        Iterates through the active member list and updates the sender and receiver table.
        :return: void
        '''
        node_p = self.self_node.pref            #servers that this node will be listening heartbeats from
        node_n = self.self_node.nref            #servers that this node will be sending heartbeats to
        new_sender = set()
        new_receiver = set()
        for _ in range(self.n_size):

            if node_p == self.self_node:         #iterated the whole system. No more to add, break
                break
            else:
                new_sender.add(node_n.item)
                new_receiver.add(node_p.item)
                node_n = node_n.nref
                node_p = node_p.pref

        self.send_table_update(new_sender)
        self.recv_table_update(new_receiver)

        return

    def send_table_update(self, new_set):  #expected value of packet = [new_ip, old_ip]
        '''
        - gets a new set of ip list to send the heartbeats to. Delete old ip if not included in the new set.
        :param new_set: set of new ip list
        :return: void
        '''
        if len(new_set)==0:
            self.log.error('send_table_update Zero length sending set. May be an error.')   # Critical Error. Membership list not properly updated.')
            #return

        self.hb_send_table_lock.acquire()
        self.log.info('send_table_update Acquiring hb_send_table_lock')
        
        del_key = 0
        for key in self.hb_send_table:     #delete old ip from the send table, if any
            if key not in new_set:
                del_key = key
        if del_key != 0:
            del self.hb_send_table[del_key]

        set_send = set(self.hb_send_table.keys())
        for key in (new_set - set_send):             # add new ip to the send table
            self.hb_send_table[key] = 0               # counter = 0 for first heartbeat

        self.log.info('send_table_update Releasing hb_send_table_lock')
        self.hb_send_table_lock.release()

    def recv_table_update(self, new_set):    #expected value of packet = [new_ip, old_ip]
        '''
        - updates or adds an element in hb_recv_table. If old ip address value is given,
           replaces the old ip address with new ip address given.
        :param packet:
        :param address:
        :return:
        '''
        if len(new_set)==0:
            self.log.error('recv_table_update Zero length receiving set. May be an error.') #Critical Error. Membership list not properly updated.')
            #return

        self.log.info('recv_table_update Acquiring hb_recv_table_lock')
        self.hb_recv_table_lock.acquire()
        
        del_key = 0
        for key in self.hb_recv_table:                  #delete old ip from the send table, if any
            if key not in new_set:
                del_key = key
        if del_key != 0:
            del self.hb_recv_table[del_key]

        set_send = set(self.hb_recv_table.keys())
        for key in (new_set - set_send):                           # add new ip to the send table
            self.hb_recv_table[key] = (0, time.time())                             #counter = 0 for first listening

        self.hb_recv_table_lock.release()
        self.log.info('recv_table_update Releasing hb_recv_table_lock')

    def update_hb_count(self):
        '''
        increment the counter of all sending server and return the dictionary of target ip addresses and incremented counter value
        :return: self.hb_send_table
        '''
        for key in self.hb_send_table.keys():
            self.hb_send_table[key] += 1
        return self.hb_send_table

    def get_host_name_ip(self):
        '''
        Returns a tuple of host_name and host_ip of the current server
        :return: (host_name, host_ip)
        '''
        try:
            host_name = socket.gethostname()
            host_ip = socket.gethostbyname(host_name)
            return host_name, host_ip
        except:
            print("Unable to get Hostname and IP")
            return None

    def ret_membership_list(self):
        self.active_node_list_lock.acquire()
        ret = self.active_node_list.return_list()
        self.active_node_list_lock.release()
        return ret

    def ret_membership_id_ls(self):
        '''
        Function for to be called by other objects to get the current membership list
        :return: ["hostname", "hostname" ... ]
        '''
        self.active_node_list_lock.acquire()
        ret = self.active_node_list.traverse_list_id_ls()
        self.active_node_list_lock.release()

        return ret

    def ret_self_id(self):
        return self.self_node.id