def sol2(a):
    A1=[]
    B1=[]
    product1=1
    product2=1
    for i in range(1,len(a)):
        if i==1:
            A1.append(1)
        p=i
        while (p>0):
            product1*=a[p-1]
            p=p-1
        A1.append(product1)
        product1=1
    print(A1)

    for i in range(0,len(a)):
       if i==len(a)-1:
           B1.append(1)
       else:
           p=len(a)
           while(p-i!=1):
               product2*=a[p-1]
               p=p-1
           B1.append(product2)
           product2=1
    print(B1)
    c1=[a*b for a,b in zip(A1,B1)]
    print c1

sol2([1,2,3,4])
sol2([8,4,3,2])
sol2([0,0,-9,100])
sol2([0,45,32,100])
sol2([9,78,45,6767,98,34,9,88,45])





