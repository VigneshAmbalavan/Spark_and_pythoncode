class node():
    def __init__(self,data=None):
        self.data=data
        self.next=None

class linked_list():
    def __init__(self):
        self.head=node()

###appending at the end of the list
    def append_last(self,data):
        new_node=node(data)
        cur_node=self.head
        while (cur_node.next!=None):
            cur_node=cur_node.next
        cur_node.next=new_node

###appending at the head of the list
    def append_begining(self,data):
        new_node=node(data)
        cur_node=self.head
        present_next_node=cur_node.next
        cur_node.next=new_node
        new_node.next=present_next_node

   ###for displaying the elements of the list#####
    def display(self):
         ele=[]
         cur_node=self.head
         while (cur_node.next!=None):
             cur_node=cur_node.next
             ele.append(cur_node.data)
         print(ele)

   ####calculatin the length of the linked_list
    def length(self):
        self.i=0
        cur_node=self.head
        while (cur_node.next!=None):
            cur_node=cur_node.next
            self.i+=1
        return self.i


####getting the data of ith node#####
    def get_data(self,j):
        self.j=j
        index=j
        if index < 0:
            print("ERROR: can't have the negative index")
            return  None
        if index >= self.length():
            print("ERROR: no index present ")

            return None
        cur_index=0
        cur_node=self.head
        while True:
            cur_node=cur_node.next
            if cur_index==index: return cur_node.data
            cur_index+=1


###appendingg at the ith index #######
    def append_index_place(self,i,data):
        self.i=i
        self.data=data
        if i >= self.length():
            print("as the length of list itself is %d and %d is greater index than that so appending at last"%(self.length(),i))
            self.append_last(data)
            return None
        if i < 0:
            print("can't have negative index")
        cur_node=self.head
        cur_index=0
        while (cur_index!=i):
            cur_node=cur_node.next
            cur_index+=1
        present_node_next = cur_node.next
        new_node = node(data)
        cur_node.next = new_node
        new_node.next = present_node_next

###removing from ith position in linked_list####

    def remove_index_place(self,i):
        self.i = i
        if i >= self.length():
            print("ERROR: the length of list itself is %d and %d is greater index than that so can't remove anything"%(self.length(),i))
            return None
        if i < 0:
            print("can't have negative index")
            return None
        cur_node = self.head
        cur_index = 0
        while (cur_index!=i+1):
              last_node = cur_node
              cur_node = cur_node.next
              cur_index+=1
        present_node_next = cur_node.next
        last_node.next=present_node_next











my_list=linked_list()
my_list.append_last(1)
my_list.append_last(7)
my_list.append_last(8)
my_list.append_begining(9)
# print(my_list.length())
my_list.display()
# my_list.get_data(-2)
# my_list.get_data(10)
print(my_list.get_data(2))
my_list.append_index_place(100,90)
my_list.display()
my_list.append_index_place(3,100)
my_list.display()
my_list.append_index_place(0,1)
my_list.display()
my_list.remove_index_place(2)
my_list.display()
my_list.remove_index_place(0)
my_list.display()
my_list.remove_index_place(-1)
my_list.display()
my_list.remove_index_place(1)
my_list.display()
my_list.remove_index_place(3)
my_list.display()

