MOD = 10**9 + 7

def main():
    n = int(input())
    if n < 2:
        print(3)
    result = 3**n % MOD
    loai = 3**(n - 2) % MOD
    loai *= (3**(n - 3) * 2) ** (n - 2) % MOD
    print((result - loai) % MOD)
        
main()