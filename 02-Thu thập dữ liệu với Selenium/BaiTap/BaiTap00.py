from selenium import webdriver
from selenium.webdriver.common.by import By
import time

# Tao 1 driver de bat dau dieu khien
driver = webdriver.Chrome()

# Mo mot trang web
driver.get("https://gomotungkinh.com/")
# Doi 5 giay de tai trang web
time.sleep(5)

try:
    while True:
        driver.find_element(By.ID, "bonk").click()
        # Tam dung 1 giay
        time.sleep(1)
except:
    driver.quit()
    
