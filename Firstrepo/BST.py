class node():
	def __init__(self,value=None):
	    self.value=value
	    self.left_child=None
	    self.right_child=None

class BST():
	def __init__(self):
		self.root=None
	def insert(self,value):
		if self.root is None:
			self.root=node(value)
		else:
			self._insert(value,self.root)
	def _insert(self,value,cur_node):
		if value<cur_node:
			if cur_node.left_child is None:
				cur_node.left_child=node(value)
			else:
				cur_node=cur_node.left_child
				self._insert(value,cur_node)
		elif value>cur_node:
			if cur_node.right_child is None:
				cur_node.right_child=node(value)
			else:
				cur_node=cur_node.right_child
				self._insert(value, cur_node)
		else:
			print("this %d value is already present in the tree"%value)
	def print_BST(self):
		if self.root==None:
			return None
		else:
			self._print(self.root)


t=BST()
from random import randint
for x in range(10):

    t.insert(randint(1,10))




