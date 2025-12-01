from pygments.formatters.html import webify
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import re
import time

d = pd.DataFrame({
    "name": [],
    "birth": [],
    "death": [],
    "nationality": [],
})

all_links = []

driver = webdriver.Chrome()

try:
    for i in range(70, 71):
        url = f'https://en.wikipedia.org/wiki/List_of_painters_by_name_beginning_with_%22{chr(i)}%22'
        driver.get(url)
        time.sleep(5)

        # Các <li> trong nội dung chính
        li_tags = driver.find_elements(By.CSS_SELECTOR, "div.mw-parser-output ul li")

        for li in li_tags:
            try:
                a = li.find_element(By.TAG_NAME, "a")
                href = a.get_attribute("href")
                # Chỉ lấy link bài wiki bình thường
                if href and href.startswith("https://en.wikipedia.org/wiki/") and "List_of_" not in href:
                    if href not in all_links:
                        all_links.append(href)
                        if len(all_links) >= 3:
                            break
            except:
                pass

        if len(all_links) >= 3:
            break
finally:
    driver.quit()

all_links = list(dict.fromkeys(all_links))

for idx, link in enumerate(all_links, start=1):
    print(idx, "/", len(all_links), ":", link)

    driver = webdriver.Chrome()
    try:
        driver.get(link)
        time.sleep(5)

        # name = title trang
        try:
            name = driver.find_element(By.TAG_NAME, "h1").text
        except:
            name = ""

        # birth
        try:
            birth_element = driver.find_element(
                By.XPATH, "//th[text()='Born']/following-sibling::td"
            )
            birth_text = birth_element.text
            m = re.findall(r"[0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4}", birth_text)
            birth = m[0]
        except:
            birth = ""

        # death
        try:
            death_element = driver.find_element(By.XPATH, "//th[text()='Died']/following-sibling::td")
            death_text = death_element.text
            m = re.findall(r"[0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4}", death_text)
            death = m[0]
        except:
            death = ""

        # nationality
        try:
            nationality_element = driver.find_element(By.XPATH, "//th[text()='Nationality']/following-sibling::td")
            nationality = nationality_element.text
        except:
            nationality = ""

    finally:
        driver.quit()

    painter = {
        "name": name,
        "birth": birth,
        "death": death,
        "nationality": nationality,
    }

    d = pd.concat([d, pd.DataFrame([painter])], ignore_index=True)

print(d.head())

# Lưu Excel
d.to_excel("painters.xlsx", index=False)
print("Đã lưu file painters.xlsx")

# tim danh sach cac truong dai hoc cao dang tu (ten truong, ma truong, ten hieu truong, url)