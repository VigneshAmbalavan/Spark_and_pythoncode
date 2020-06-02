# Definition for a binary tree node.
class TreeNode(object):
    def __init__(self, val=0, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right
class Solution (object):
    def swap(self, root):
        if root is None:
            return
        self.swap(root.left)
        self.swap(root.right)

        tmp = TreeNode ()
        tmp = root.left
        root.left = root.right
        root.right = tmp

    def invertTree(self, root):
        """
        :type root: TreeNode
        :rtype: TreeNode
        """
        self.swap(root)
        return root


