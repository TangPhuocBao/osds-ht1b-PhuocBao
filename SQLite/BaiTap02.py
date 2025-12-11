import sqlite3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import pandas as pd
import re
import os # Thêm thư viện để kiểm tra/xóa file DB (tùy chọn)

######################################################
## I. Cấu hình và Chuẩn bị
######################################################

# Thiết lập tên file DB và Bảng
DB_FILE = 'SQLite/Painters_Data.db'
TABLE_NAME = 'painters_info'
all_links = []

# Tùy chọn cho Chrome (có thể chạy ẩn nếu cần, nhưng để dễ debug thì không dùng)
# chrome_options = Options()
# chrome_options.add_argument("--headless") 

# Nếu muốn bắt đầu với DB trống, có thể xóa file cũ (Tùy chọn)
if os.path.exists(DB_FILE):
    os.remove(DB_FILE)
    print(f"Đã xóa file DB cũ: {DB_FILE}")

# Mở kết nối SQLite và tạo bảng nếu chưa tồn tại
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# Tạo bảng
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    name TEXT PRIMARY KEY, -- Sử dụng tên làm khóa chính để tránh trùng lặp
    birth TEXT,
    death TEXT,
    nationality TEXT
);
"""
cursor.execute(create_table_sql)
conn.commit()
print(f"Đã kết nối và chuẩn bị bảng '{TABLE_NAME}' trong '{DB_FILE}'.")

# Hàm đóng driver an toàn
def safe_quit_driver(driver):
    try:
        if driver:
            driver.quit()
    except:
        pass

######################################################
## II. Lấy Đường dẫn (URLs)
######################################################

print("\n--- Bắt đầu Lấy Đường dẫn ---")

# Lặp qua ký tự 'F' (chr(70))
try:
    for i in range(70, 71):
        driver = webdriver.Chrome()
        url = f'https://en.wikipedia.org/wiki/List_of_painters_by_name_beginning_with_%22{chr(i)}%22'
        driver.get(url)
        time.sleep(5)
        all_links.append(url)

        if len(all_links) >= 50:
            break
finally:
    safe_quit_driver(driver)


######################################################
## III. Lấy thông tin & LƯU TRỮ TỨC THỜI
######################################################

print("\n--- Bắt đầu Cào và Lưu Trữ Tức thời ---")
count = 0
for link in all_links:
    driver = webdriver.Chrome()
    driver.get(link)
    li_tags = driver.find_elements(By.CSS_SELECTOR, "div.mw-parser-output ul li")
    # Giới hạn số lượng truy cập để thử nghiệm nhanh
    if (count >= 50): # Đã tăng lên 5 họa sĩ để có thêm dữ liệu mẫu
        break
    count = count + 1
    driver = None
    for li in li_tags[26:]:
        text = li.text.strip()
        try:
            driver = webdriver.Chrome() 
            driver.get(link)
            time.sleep(2)

            # 1. Lấy tên họa sĩ
            for li in li_tags:
                try :
                    name = text.split("(")[0].strip()
                except:
                    name = ""
            
            # 2. Lấy ngày sinh (Born)
            birth = None
            death = None
            # Lấy phần trong ngoặc (xxxx–yyyy) hoặc (born yyyy)
            year_part = re.search(r"\(([^)]*)\)", text)
            if year_part:
                year_text = year_part.group(1).lower()
                year_text = year_text.split(",")[0].strip()  # Loại bỏ phần mô tả sau dấu phẩy
            try:
                # born 1984
                if "born" in year_text:
                    birth = re.search(r"\d{4}", year_text).group()
                    death = None

                # dạng 1822–1854
                elif "–" in year_text or "-" in year_text:
                    years = re.split(r"[–]", year_text)
                    if len(years) == 2:
                        birth = re.search(r"\d{4}", years[0]).group()
                        death = re.search(r"\d{4}", years[1]).group()

                # dạng (1984)
                elif re.fullmatch(r"\d{4}", year_text):
                    birth = year_text
                    death = None
            except:
                birth = ""
                death = ""
            # 4. Lấy quốc tịch (Nationality)
            try:
        # Quốc tịch (từ sau dấu phẩy đầu tiên)
                nationality = ""
                if "," in text:
                    nationality = text.split(",", 1)[1].strip()
                    nationality = nationality.split(" ")[0].strip()  # Loại bỏ phần mô tả sau dấu chấm
            except:
                nationality = ""

            safe_quit_driver(driver)
            
            # 5. LƯU TỨC THỜI VÀO SQLITE
            insert_sql = f"""
            INSERT OR IGNORE INTO {TABLE_NAME} (name, birth, death, nationality) 
            VALUES (?, ?, ?, ?);
            """
            # Sử dụng 'INSERT OR IGNORE' để bỏ qua nếu Tên (PRIMARY KEY) đã tồn tại
            cursor.execute(insert_sql, (name, birth, death, nationality))
            conn.commit()
            print(f"  --> Đã lưu thành công: {name}")

        except Exception as e:
            print(f"Lỗi khi xử lý hoặc lưu họa sĩ {link}: {e}")
            safe_quit_driver(driver)
            
print("\nHoàn tất quá trình cào và lưu dữ liệu tức thời.")

######################################################
## IV. Truy vấn SQL Mẫu và Đóng kết nối
######################################################

"""
A. Yêu Cầu Thống Kê và Toàn Cục
1. Đếm tổng số họa sĩ đã được lưu trữ trong bảng.
2. Hiển thị 5 dòng dữ liệu đầu tiên để kiểm tra cấu trúc và nội dung bảng.
3. Liệt kê danh sách các quốc tịch duy nhất có trong tập dữ liệu.

B. Yêu Cầu Lọc và Tìm Kiếm
4. Tìm và hiển thị tên của các họa sĩ có tên bắt đầu bằng ký tự 'F'.
5. Tìm và hiển thị tên và quốc tịch của những họa sĩ có quốc tịch chứa từ khóa 'French' (ví dụ: French, French-American).
6. Hiển thị tên của các họa sĩ không có thông tin quốc tịch (hoặc để trống, hoặc NULL).
7. Tìm và hiển thị tên của những họa sĩ có cả thông tin ngày sinh và ngày mất (không rỗng).
8. Hiển thị tất cả thông tin của họa sĩ có tên chứa từ khóa '%Fales%' (ví dụ: George Fales Baker).

C. Yêu Cầu Nhóm và Sắp Xếp
9. Sắp xếp và hiển thị tên của tất cả họa sĩ theo thứ tự bảng chữ cái (A-Z).
10. Nhóm và đếm số lượng họa sĩ theo từng quốc tịch.
"""
# 1A. Đếm tổng số họa sĩ
sql1a = "SELECT COUNT(*) FROM painters_info;"
cursor.execute(sql1a)
conn.commit()
total_painters = cursor.fetchone()[0]
print(f"\n1. Tổng số họa sĩ trong bảng: {total_painters}")

# 2A. Hiển thị 5 dòng dữ liệu đầu tiên
sql2a = "SELECT * FROM painters_info LIMIT 5;"
cursor.execute(sql2a)
conn.commit()
rows = cursor.fetchall()
print("\n2. 5 dòng dữ liệu đầu tiên:")
for row in rows:
    print(row)

# 3A. Liệt kê quốc tịch duy nhất
sql3a = '''
SELECT DISTINCT nationality FROM painters_info
WHERE nationality IS NOT NULL AND nationality != '';
'''
cursor.execute(sql2a)
conn.commit()
rows = cursor.fetchall()
for row in rows:
    print(row)

# 4B. Họa sĩ có tên bắt đầu bằng 'F'
sql4b = '''
SELECT name FROM painters_info
WHERE name LIKE '% F%';
'''
cursor.execute(sql2a)
conn.commit()
rows = cursor.fetchall()
for row in rows:
    print(row)

# 5B. Họa sĩ có quốc tịch chứa 'French'
sql5b = '''
SELECT name, nationality FROM painters_info
WHERE nationality LIKE '%French%';
'''
cursor.execute(sql2a)
conn.commit()
rows = cursor.fetchall()
for row in rows:
    print(row)

# 6B. Họa sĩ không có thông tin quốc tịch
sql6b = '''
SELECT name FROM painters_info
WHERE nationality IS NULL OR nationality = '';
'''
cursor.execute(sql2a)
conn.commit()
rows = cursor.fetchall()
for row in rows:
    print(row)

# 7B. Họa sĩ có cả ngày sinh và ngày mất
sql7b = '''
SELECT name FROM painters_info
WHERE birth IS NOT NULL AND birth != '' AND death IS NOT NULL AND death != '';
'''
cursor.execute(sql2a)
conn.commit()
rows = cursor.fetchall()
for row in rows:
    print(row)

# 8B. Họa sĩ có tên chứa 'Fales'
sql8b = '''
SELECT * FROM painters_info
WHERE name LIKE '%Fales%';
'''
cursor.execute(sql2a)
conn.commit()
rows = cursor.fetchall()
for row in rows:
    print(row)

# 9C. Sắp xếp họa sĩ theo tên A-Z
sql9c = '''
SELECT name FROM painters_info
ORDER BY name ASC;
'''
cursor.execute(sql2a)
conn.commit()
rows = cursor.fetchall()
for row in rows:
    print(row)

# 10C. Nhóm và đếm số họa sĩ theo quốc tịch
sql10c = '''
SELECT nationality, COUNT(*) as [painter_count FROM painters]
GROUP BY nationality;
'''
cursor.execute(sql2a)
conn.commit()
rows = cursor.fetchall()
for row in rows:
    print(row)

# Đóng kết nối cuối cùng
conn.close()
print("\nĐã đóng kết nối cơ sở dữ liệu.")