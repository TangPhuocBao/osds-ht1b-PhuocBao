from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver import ActionChains
import time
import pandas as pd
import getpass

# Khởi tạo Firefox driver
options = webdriver.FirefoxOptions()
service = Service(r"C:/KPDL/gecko/geckodriver.exe")
options.binary_location = "C:/Program Files/Mozilla Firefox/firefox.exe"
driver = webdriver.Firefox(service=service, options=options)

# Truy cập LMS
driver.get("https://apps.lms.hutech.edu.vn/authn/login?next")

# Nhập email và password
email = input("Email: ")
password = getpass.getpass("Password: ")

time.sleep(2)  # đợi trang load

driver.find_element(By.NAME, "emailOrUsername").send_keys(email)
driver.find_element(By.NAME, "password").send_keys(password)

# Nhấn nút login
driver.find_element(By.ID, "sign-in").click()

time.sleep(5)  # đợi đăng nhập

print("Đăng nhập xong!")
driver.quit()
