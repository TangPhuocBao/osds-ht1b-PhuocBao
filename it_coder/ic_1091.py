n, m = map(int, input().split())
an = m // n
if m < n * (n + 1) / 2:
    print(-1)
else:
    a1 = an - n + 1
    cl = (n * (n - 1) / 2) + m % n
    up = int(cl // n)
    an = an + 1 if cl % n else an
    print(a1 + up, an + up)