import bisect

def main(k):
    result=[]
    for i in range(len(k)):
        j=i+1
        if j==len(k):
            result.append(-1)
            continue
        a_new=k[j::]
        if k[i]>max(a_new):
            result.append(-1)
        elif k[i]=<max(a_new):
            result.append(int(max(a_new)))

    return result



if __name__=='__main__':
    num_time_fun_call=int(raw_input())

    for j in range(num_time_fun_call,0,-1):
        len_array = raw_input()
        k = [int(x) for x in raw_input().split()]
        print(k)
        res=main(k)
        l=''
        for i in res:
            l+="%d "%(i)
        print(l)