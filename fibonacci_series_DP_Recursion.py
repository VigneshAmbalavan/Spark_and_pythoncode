
#####inefficient Recursion Method###########
def fib_rec(n):
    if n == 0:
        return 0
    elif n==1:
        return 1
    else:
        k = fib_rec(n - 1) + fib_rec(n - 2)
        print("Agam")
    return k
###using DP memoization(top bottom)#######
def fib_dp_memo(n,dp):
    if n==0 or n==1:
       dp[n]=n
    if dp[n] is None:
        dp[n]=fib_dp_memo(n-2,dp)+fib_dp_memo(n-1,dp)
    return  dp[n]

def main(n):
    dp=[None]*(n+1)
    #print(fib_dp_memo(n,dp))
    print(fib_rec(n))


if __name__=='__main__':
    main(5)