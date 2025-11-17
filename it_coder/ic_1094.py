MOD = 10**9 + 7

n = int(input())
if n == 0:
    print(0)  # không có dãy nào độ dài 0
else:
    result = pow(3, n - 1, MOD)
    print(result)