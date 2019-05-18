def queue_even(k):
    if len(k)==1:
        result=k[0]
        print result
        return
    g=[]
    for i in range(0,len(k)):
        if i%2==0:
            g.append(i)
        else:
            continue
    queue_even(g)

queue_even([2,3,4,5,6,7,8,9,10])


