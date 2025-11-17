import sys

def last_digit(base, exp):
    if exp == "0":
        return 1

    base = base[-1]
    cycles = {
        '0': [0],
        '1': [1],
        '2': [2, 4, 8, 6],
        '3': [3, 9, 7, 1],
        '4': [4, 6],
        '5': [5],
        '6': [6],
        '7': [7, 9, 3, 1],
        '8': [8, 4, 2, 6],
        '9': [9, 1]
    }

    cycle = cycles[base]
    mod = len(cycle)

    idx = 0
    for c in exp:
        idx = (idx * 10 + int(c)) % mod

    return cycle[idx - 1]

def main():
    line = sys.stdin.read().split()
    base, exp = line
    print(last_digit(base, exp))
        
main()
