from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import pandas as pd
import getpass

LINKEDIN_EMAIL = input("EMAIL: ")
LINKEDIN_PASS = getpass.getpass("PASS: ")
KEYWORD = "python developer"

# Khởi tạo trình duyệt
driver = webdriver.Chrome()

try:
    # ĐĂNG NHẬP
    print("Đang truy cập trang đăng nhập...")
    driver.get("https://www.linkedin.com/login")
    time.sleep(2)

    # Điền Email
    driver.find_element(By.ID, "username").send_keys(LINKEDIN_EMAIL)
    
    # Điền Password
    driver.find_element(By.ID, "password").send_keys(LINKEDIN_PASS)
    
    # Nhấn nút Sign in
    driver.find_element(By.XPATH, "//button[@type='submit']").click()
    
    print("Đã gửi thông tin đăng nhập.")
    time.sleep(15) # Thời gian chờ để trang chủ load hoặc để bạn giải CAPTCHA

    # TÌM KIẾM
    print(f"Đang tìm kiếm: {KEYWORD}")
    driver.get(f"https://www.linkedin.com/search/results/content/?keywords={KEYWORD}")
    time.sleep(5)

    # CUỘN TRANG
    print("Đang cuộn trang")
    for i in range(2): # Cuộn 2 lần
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)

    # LẤY DỮ LIỆU
    print("Đang thu thập bài viết...")
    posts = driver.find_elements(By.XPATH, "//div[contains(@class, 'feed-shared-update-v2')]")

    data = []
    for post in posts:
        try:
            content = post.text
            # Lấy tên tác giả (nếu tìm thấy)
            try:
                author = post.find_element(By.CLASS_NAME, "feed-shared-actor__name").text
            except:
                author = "Unknown"

            if content:
                data.append({
                    "Author": author,
                    "Content": content
                })
        except:
            continue

    # LƯU FILE
    if data:
        df = pd.DataFrame(data)
        filename = f"linkedin_auto_{KEYWORD.replace(' ', '_')}.csv"
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Xong! Đã lưu {len(data)} bài viết vào {filename}")
    else:
        print("Không lấy được bài viết nào.")

except Exception as e:
    print(f"Lỗi: {e}")

finally:
    driver.quit()