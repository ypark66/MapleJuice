class Node:
    def __init__(self, data):
        self.item = data
        self.id = None
        self.nref = None
        self.pref = None


class DoublyLinkedList:
    def __init__(self):
        self.start_node = None
        self.tail_node = None

    def insert_in_emptylist(self, data):
        if self.start_node is None:
            new_node = Node(data)
            self.start_node = new_node
            return new_node
        else:
            print("list is not empty")


    def insert_at_start(self, data):
        if self.start_node is None:
            new_node = Node(data)
            self.start_node = new_node
            print("node inserted")
            return
        new_node = Node(data)
        new_node.nref = self.start_node
        self.start_node.pref = new_node
        self.start_node = new_node


    def insert_at_end(self, data):
        if self.start_node is None:
            new_node = Node(data)
            self.start_node = new_node
            return
        if self.tail_node == None:
            n = self.start_node
            while n.nref is not None:
                n = n.nref
            new_node = Node(data)
            n.nref = new_node
            new_node.pref = n
            self.tail_node = new_node
            return new_node
        else:
            new_node = self.insert_after_node(self.tail_node,data)
            self.tail_node = new_node
            return new_node

    def insert_after_node(self, x, data):
        if self.start_node is None:
            print("List is empty")
            return
        else:
            new_node = Node(data)
            new_node.pref = x
            new_node.nref = x.nref
            if x.nref is not None:
                x.nref.pref = new_node
            x.nref = new_node
            return new_node

    def insert_after_item(self, x, data):
        if self.start_node is None:
            print("List is empty")
            return
        else:
            n = self.start_node
            while n is not None:
                if n.item == x:
                    break
                n = n.nref
            if n is None:
                print("item not in the list")
            else:
                new_node = Node(data)
                new_node.pref = n
                new_node.nref = n.nref
                if n.nref is not None:
                    n.nref.prev = new_node
                n.nref = new_node
                return new_node


    def insert_before_item(self, x, data):
        if self.start_node is None:
            print("List is empty")
            return
        else:
            n = self.start_node
            while n is not None:
                if n.item == x:
                    break
                n = n.nref
            if n is None:
                print("item not in the list")
            else:
                new_node = Node(data)
                new_node.nref = n
                new_node.pref = n.pref
                if n.pref is not None:
                    n.pref.nref = new_node
                n.pref = new_node

    def traverse_list(self):
        if self.start_node is None:
            print("List has no element")
            return
        else:
            n = self.start_node
            while n is not None:
                print(n.item , " ")
                n = n.nref
                if n == self.start_node:
                    break

    def traverse_list_id_ls(self):
        ret = []

        if not self.start_node is None:
            n = self.start_node
            while n is not None:
                ret.append(n.id)
                n = n.nref
                if n == self.start_node:
                    break

        return ret

    def get_all_id(self):
        if self.start_node is None:
            print("List has no element")
            return
        else:
            n = self.start_node
            while n is not None:
                print(n.id, " ")
                n = n.nref
                if n == self.start_node:
                    break

    def return_list(self):
        if self.start_node is None:
            print("List has no element")
            return None
        else:
            list = []
            n = self.start_node
            while n is not None:
                list.append(n.item)
                n = n.nref
                if n == self.start_node:
                    break
            return list


    def return_list_with_id(self):
        if self.start_node is None:
            print("List has no element")
            return None
        else:
            list = []
            n = self.start_node
            while n is not None:
                list.append((n.item,n.id))
                n = n.nref
                if n == self.start_node:
                    break
            return list



    def delete_at_start(self):
        if self.start_node is None:
            print("The list has no element to delete")
            return
        if self.start_node.nref is None:
            self.start_node = None
            return
        self.start_node = self.start_node.nref
        self.start_prev = None


    def delete_at_end(self):
        if self.start_node is None:
            print("The list has no element to delete")
            return
        if self.start_node.nref is None:
            self.start_node = None
            return
        n = self.start_node
        while n.nref is not None:
            n = n.nref
        n.pref.nref = None

    def check_if_in(self, x):
        # Check if an element is in doubly linked list
        ret = False

        if not self.start_node is None:
            target = self.start_node
            while True:
                if target.item == x:
                    ret = True
                    break

                if target.nref is None or\
                    target.nref == self.start_node:
                    break
                    
                target = target.nref

        return ret

    def delete_element_by_value(self, x):
        if self.start_node is None:
            print("The list has no element to delete")
            return
        if self.start_node.nref is None:
            if self.start_node.item == x:
                self.start_node = None
            else:
                print("Item not found")
            return

        # should also deal with the case that the doubly linked list is also a circular one
        if self.start_node.item == x:
            if self.start_node.pref is None:
                # not circular linked list, maintain as is
                self.start_node = self.start_node.nref
                self.start_node.pref = None
            else:
                # this is a circular linked list, maintain as is
                self.start_node.nref.pref = self.start_node.pref
                self.start_node.pref.nref = self.start_node.nref
                self.start_node = self.start_node.nref
            return

        n = self.start_node
        while n.nref is not None:
            if n.item == x:
                break
            n = n.nref
        if n.nref is not None:
            n.pref.nref = n.nref
            n.nref.pref = n.pref
        else:
            if n.item == x:
                n.pref.nref = None
            else:
                print("Element not found")

    def reverse_linked_list(self):
        if self.start_node is None:
            print("The list has no element to delete")
            return
        p = self.start_node
        q = p.nref
        p.nref = None
        p.pref = q
        while q is not None:
            q.pref = q.nref
            q.nref = p
            p = q
            q = q.pref
        self.start_node = p
