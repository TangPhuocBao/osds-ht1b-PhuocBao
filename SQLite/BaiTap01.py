import sqlite3

# Kết nối đến cơ sở dữ liệu SQLite (nếu không tồn tại sẽ tạo mới)
conn = sqlite3.connect('SQLite/inventory.db')

# Tạo đối tượng con tró cursor để thực thi các lệnh SQL
cursor  = conn.cursor()

### Thao tác database and table
# Tạo bảng products
sql1 = '''
CREATE TABLE IF NOT EXISTS products(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    quantity INTEGER,
    price REAL
)
'''
# Thực thi lệnh SQL
cursor.execute(sql1)
conn.commit() # Lưu thay đổi vào DB

# 3.CRUD - Create, Read, Update, Delete
# Create - Thêm dữ liệu vào bảng products
products = [
    ('Apple', 50, 0.5),
    ('Banana', 100, 0.2),
    ('Orange', 80, 0.3)
]

# Lệnh SQL để chèn dữ liệu. DÙng '?' để tránh lỗi SQL Injection
sql2 = '''
INSERT INTO products (name, quantity, price)
VALUES (?, ?, ?)
'''

# Thêm nhiều sản phảm cùng lúc
cursor.executemany(sql2, products)
conn.commit() # Lưu thay đổi vào DB

# Read - Đọc dữ liệu từ bảng products
sql3 = '''
SELECT * FROM products
'''

# Thực thi lệnh SQL và lấy kết quả
cursor.execute(sql3)
rows = cursor.fetchall()

# Lặp và in ra
for row in rows:
    print(row)
    
# 3.3 Update - Cập nhật dữ liệu
price_product_name_update = [('Apple')]
sql4 = '''
UPDATE products
SET price = price * 0.2
WHERE name = ?
'''

cursor.execute(sql4, price_product_name_update)
conn.commit() # Lưu thay đổi vào DB

# 3.4 Delete - Xóa dữ liệu
delete_product_name = [('Banana')]
sql5 = """
DELETE
FROM products
WHERE name = ?
"""
cursor.execute(sql5, delete_product_name)
conn.commit() # Lưu thay đổi vào DB


