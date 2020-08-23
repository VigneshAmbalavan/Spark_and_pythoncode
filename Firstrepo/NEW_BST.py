
class node():
    def __init__(self,data=None):
        self.data=data
        self.left=None
        self.right=None

class BST():
    def __init__(self):
        self.root=None

    def insert(self,value):
        if self.root is None:
            self.root=node(value)
        else:
             cur_node=self.root
             self._insert (cur_node, value)
    def _insert(self,cur_node,value):
        if value<cur_node.data:
            if cur_node.left is None:
                cur_node.left=node(value)
            else:
                cur_node=cur_node.left
                self._insert(cur_node,value)
        elif value>cur_node.data:
            if cur_node.right is None:
                cur_node.right=node(value)
            else:
                cur_node=cur_node.right
                self._insert (cur_node, value)
        elif value==cur_node.data:
            print("can't insert as data is already present")

    def find(self,value):
        if self.root.data==value:

            return True
        elif self.root is None:
            print("no data in BST")
            return None
        else:

            cur_node=self.root
            res=self._find(cur_node,value)

            return res

    def _find(self,cur_node,value):
        if value==cur_node.data:
            return True
        elif value<cur_node.data and cur_node.left!=None:
            self._find(cur_node.left,value)
        elif value>cur_node.data and cur_node.right!=None:
            self._find(cur_node.right,value)
        return False

    def display_inorder(self):
        if self.root is None:
            print("nothing to display as BST is empty")
        else:
            cur_node=self.root
            self._display_inorder(cur_node)
    def _display_inorder(self,cur_node):
        if cur_node!=None:
            self._display_inorder(cur_node.left)
            print(cur_node.data)
            self._display_inorder(cur_node.right)



t=BST()
t.insert(7)
t.insert(5)
t.insert(8)
t.insert(11)
# t.insert(11)
# print("piya")
y=t.find(11)
print(y)
t.display_inorder()
# from random import randint
# for x in range(10):
#
# 	t.insert(randint(1,10))
