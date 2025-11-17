def prime_factors(n: int) -> str:
    if n <= 3:
        return 'NO'

    tich = 1
    temp = n

    for p in (2, 3):
        if temp % p == 0:
            tich *= p
            if tich > n:
                return 'NO'
            while temp % p == 0:
                temp //= p

    i = 5
    while i * i <= temp:
        for p in (i, i + 2):
            if p * p > temp:
                break
            if temp % p == 0:
                tich *= p
                if tich > n:
                    return "NO"
                while temp % p == 0:
                    temp //= p
        i += 6

    if temp > 1:
        tich *= temp

    return "YES" if tich < n else "NO"

def main():
    n = int(input())
    print(prime_factors(n))
    
main()