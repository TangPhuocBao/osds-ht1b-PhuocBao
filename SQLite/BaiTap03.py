from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time, random
import pandas as pd
import os
import sqlite3

DB_FILE = "longchau_db.sqlite"
TABLE_NAME = "VITAMIN"

# TẠO TABLE SQLITE

conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    MA_SP TEXT ,
    TEN_SP TEXT,
    GIA_BAN REAL,
    GIA_GOC REAL,
    DVT TEXT,
    URL PRIMARY KEY TEXT
);
""")
conn.commit()
gecko_path = r"C:/KPDL/gecko/geckodriver.exe"
service = Service(gecko_path)


options = webdriver.FirefoxOptions()
options.binary_location = r"C:/Program Files/Mozilla Firefox/firefox.exe"

driver = webdriver.Firefox(service=service, options=options)

# TRUY CẬP TRANG

url = "https://nhathuoclongchau.com.vn/thuc-pham-chuc-nang/vitamin-khoang-chat"
driver.get(url)
time.sleep(5)

body = driver.find_element(By.TAG_NAME, "body")


# CLICK "XEM THÊM"

for _ in range(12):
    try:
        buttons = driver.find_elements(By.TAG_NAME, "button")
        for bt in buttons:
            if "Xem thêm" in bt.text:
                bt.click()
                time.sleep(random.uniform(1.5, 3.0))
                break
    except:
        pass
    
# SCROLL TỰ NHIÊN
for _ in range(150):
    body.send_keys(Keys.ARROW_DOWN)
    time.sleep(random.uniform(0.3, 1.2))
time.sleep(2)


# LẤY LINK SẢN PHẨM

buttons = driver.find_elements(By.XPATH, "//button[text()='Chọn mua']")
print("Tìm thấy sản phẩm:", len(buttons))

product_links = []

for bt in buttons:
    parent = bt
    for _ in range(3):
        parent = parent.find_element(By.XPATH, "./..")

    try:
        link = parent.find_element(By.CSS_SELECTOR, "a").get_attribute("href")
        if link:
            product_links.append(link)
    except:
        pass

product_links = list(dict.fromkeys(product_links))
print("Số link duy nhất:", len(product_links))


# CLEAN GIÁ

def clean_price(t):
    if not t:
        return ""
    return t.replace("đ", "").replace(".", "").strip()


# CÀO + LƯU VÀO SQLITE

for link in product_links:

    driver.get(link)
    time.sleep(random.uniform(1.5, 3))

    try: code = driver.find_element(By.CSS_SELECTOR, "span[data-test-id='sku']").text
    except: code = ""

    try: name = driver.find_element(By.TAG_NAME, "h1").text
    except: name = ""

    try: price_sale = clean_price(driver.find_element(By.CSS_SELECTOR, "span[data-test='price']").text)
    except: price_sale = ""

    try: price_old = clean_price(driver.find_element(By.CSS_SELECTOR, "div[data-test='strike_price']").text)
    except: price_old = price_sale

    try: unit = driver.find_element(By.CSS_SELECTOR, "[data-test='unit_lv1']").text.split()[0]
    except: unit = ""

    print(name)


    # LƯU VÀO SQLITE

    cursor.execute(f"""
        INSERT INTO {TABLE_NAME}
        (MA_SP, TEN_SP, GIA_BAN, GIA_GOC, DVT, URL)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (code, name, price_sale, price_old, unit, link))

    conn.commit()

driver.quit()
conn.close()

print("toàn bộ dữ liệu đã lưu vào SQLite")

# ------------ query ------------------
# ------------ Nhóm 1 ------------------
query_dupli = """
    SELECT MA_SP, COUNT(*) AS count
    FROM VITAMIN
    GROUP BY MA_SP
    HAVING COUNT(MA_SP) > 1
"""
cursor.execute(query_dupli)
conn.commit()

query_miss = """
    SELECT *
    FROM VITAMIN
    WHERE GIA_BAN = 0 OR GIA_BAN IS NULL
"""
cursor.execute(query_miss)
conn.commit()

query_check_gia = """
    SELECT *
    FROM VITAMIN
    WHERE GIA_BAN > GIA_GOC
"""
cursor.execute(query_check_gia)
conn.commit()

query_check_unit = """
    SELECT DISTINCT DVT
    FROM VITAMIN
"""
cursor.execute(query_check_unit)
conn.commit()

query_check_crawl = """
    SELECT COUNT(*)
    FROM VITAMIN
"""
cursor.execute(query_check_crawl)
conn.commit()

# --------------- Nhóm 2 --------------
query_top10_discount = """
    SELECT MA_SP, TEN_SP , (GIA_GOC - GIA_BAN) AS [GIA_GIAM]
    FROM VITAMIN
    GROUP BY MA_SP, TEN_SP
    ORDER BY GIA_GIAM DESC
"""
cursor.execute(query_top10_discount)
conn.commit()

query_max_gb = """
    SELECT MA_SP, TEN_SP, GIA_BAN
    FROM VITAMIN
    ORDER BY GIA_BAN DESC
"""
cursor.execute(query_max_gb)
conn.commit()

query_group_dvt = """
    SELECT DVT, COUNT(DVT) AS [SO_LUONG]
    FROM VITAMIN
    GROUP BY DVT
"""
cursor.execute(query_group_dvt)
conn.commit()

query_find_name = """
    SELECT *
    FROM VITAMIN
    WHERE TEN_SP LIKE '%Vitamin C %'
"""
cursor.execute(query_find_name)
conn.commit()

query_fil_price = """
    SELECT *
    FROM VITAMIN
    WHERE GIA_BAN BETWEEN 100000 and 200000
"""
cursor.execute(query_fil_price)
conn.commit()
# ------------------ Nhóm 3 -----------------------
query_sort_gb = """
    SELECT *
    FROM VITAMIN
    ORDER BY GIA_BAN
"""
cursor.execute(query_sort_gb)
conn.commit()

query_giam_gia = """
    SELECT MA_SP, TEN_SP, ((GIA_GOC - GIA_BAN) * 100.0 / GIA_GOC) AS [GIAM_GIA]
    FROM VITAMIN
    ORDER BY GIAM_GIA DESC
    LIMIT 5
"""
cursor.execute(query_giam_gia)
conn.commit()