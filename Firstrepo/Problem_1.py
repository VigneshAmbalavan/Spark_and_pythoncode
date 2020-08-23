

class sol():
    def __init__(self,k,a):
        self.k=k
        self.a=a

    def sol1(self):
         list=[]
         for i in self.k:
             req=self.a-i
             print(req)
             if req in self.k:
                new='%d,%d'%(i,req)
                list.append(new)
         return list


t=sol([10,7,8,8,6,8,9,9],18)
print(t.sol1())


    #print(t.sol1())