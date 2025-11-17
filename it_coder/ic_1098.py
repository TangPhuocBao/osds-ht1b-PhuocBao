import sys

def func(u, v):
    u, v = int(u), int(v)
    xor = u ^ v
    if xor == 0:
        return u
    # Tìm vị trí bit cao nhất mà u và v khác nhau
    msb_pos = xor.bit_length()
    # Lấy phần giống nhau: bỏ các bit sau vị trí msb_pos
    return u >> (u.bit_length() - msb_pos) << (u.bit_length() - msb_pos)

def main():
    input = sys.stdin.read
    data = input().split()
    n = int(data[0])
    res = []
    idx = 1
    for _ in range(n):
        u, v = data[idx], data[idx + 1]
        res.append(str(func(u, v)))
        idx += 2
    sys.stdout.write('\n'.join(res))

main()
