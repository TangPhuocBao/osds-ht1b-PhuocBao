def func(a):
    n = float('inf')
    tong = 0
    for num in reversed(a):
        n = min(n - 1, num) if n > 1 else 0
        tong += n
    print(tong)

a = input()
a = list(map(int, input().split()))
func(a)

        