# Definition for singly-linked list.
# class ListNode(object):
#     def __init__(self, x=None):
#         self.val = x
#         self.next = None

class Solution (object):
    # def __init__(self):
    #     self.head=ListNode()
    def deleteNode(self, node):
        """
        :type node: ListNode
        :rtype: void Do not return anything, modify node in-place instead.
        """
        cur_node=node
        next_ele_data=node.next.val
        next_ele=cur_node.next.next
        cur_node.val=next_ele_data
        cur_node.next=next_ele