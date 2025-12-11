import sqlite3
conn = sqlite3.connect('C:/KPDL/SQLite/longchau.db')
cursor = conn.cursor()

def run_query(title, query):
    print(f"\n===== {title} =====")
    cursor.execute(query)
    conn.commit()
    rows = cursor.fetchall()
    for r in rows:
        print(r)
    print(f"--> Tổng số dòng: {len(rows)}")
# ------------ Nhóm 1 ------------------
query_dupli = """
    SELECT MA_SP, COUNT(*) AS count
    FROM VITAMIN
    GROUP BY MA_SP
    HAVING COUNT(MA_SP) > 1
"""
run_query("dupli", query_dupli)

query_miss = """
    SELECT *
    FROM VITAMIN
    WHERE GIA_BAN = 0 OR GIA_BAN IS NULL
"""
run_query('miss_value', query=query_miss)


query_check_gia = """
    SELECT *
    FROM VITAMIN
    WHERE GIA_BAN > GIA_GOC
"""
run_query("check giá", query_check_gia)

query_check_unit = """
    SELECT DISTINCT DVT
    FROM VITAMIN
"""
run_query("check dơn vị tính", query_check_unit)
query_check_crawl = """
    SELECT COUNT(*)
    FROM VITAMIN
"""
run_query("Check cào", query_check_crawl)

# --------------- Nhóm 2 --------------
query_top10_discount = """
    SELECT MA_SP, TEN_SP , (GIA_GOC - GIA_BAN) AS [GIA_GIAM]
    FROM VITAMIN
    GROUP BY MA_SP, TEN_SP
    ORDER BY GIA_GIAM DESC
    LIMIT 10
"""
run_query('Top 10 giảm giá', query_top10_discount)

query_max_gb = """
    SELECT MA_SP, TEN_SP, GIA_BAN
    FROM VITAMIN
    ORDER BY GIA_BAN DESC
"""
run_query("Sắp xếp lại theo giá bán từng lớn đến nhỏ", query_max_gb)

query_group_dvt = """
    SELECT DVT, COUNT(DVT) AS [SO_LUONG]
    FROM VITAMIN
    GROUP BY DVT
"""
run_query('Số lượng theo đơn vị tính', query_group_dvt)

query_find_name = """
    SELECT *
    FROM VITAMIN
    WHERE TEN_SP LIKE '%Vitamin C %'
"""
run_query("Tìm kiếm Vitamin C", query_find_name)

query_fil_price = """
    SELECT *
    FROM VITAMIN
    WHERE GIA_BAN BETWEEN 100000 and 200000
"""
run_query('Tìm kiếm theo giá', query_fil_price)
# ------------------ Nhóm 3 -----------------------
query_sort_gb = """
    SELECT *
    FROM VITAMIN
    ORDER BY GIA_BAN
"""
run_query("sắp xếp theo giá bán", query_sort_gb)

query_giam_gia = """
    SELECT MA_SP, TEN_SP, ((GIA_GOC - GIA_BAN) * 100.0 / GIA_GOC) AS [GIAM_GIA]
    FROM VITAMIN
    ORDER BY GIAM_GIA DESC
    LIMIT 5
"""
run_query("Top 5 giảm giá", query_giam_gia)

query_url_check = """
    SELECT *
    FROM VITAMIN
    WHERE URL IS NULL OR URL = ''
"""
run_query("URL KHÔNG HỢP LỆ", query_url_check)

query_check = f"""
SELECT
    CASE
        WHEN GIA_BAN < 50000 THEN 'Dưới 50k'
        WHEN GIA_BAN BETWEEN 50000 AND 100000 THEN '50k - 100k'
        ELSE 'Trên 100k'
    END AS nhom_gia,
    COUNT(*) AS so_san_pham
FROM VITAMIN
WHERE GIA_BAN IS NOT NULL
GROUP BY nhom_gia
"""
run_query("Nhóm giá", query_check)

delete_duplicates_sql = """
WITH to_keep AS (
    SELECT MIN(rowid) AS keep_id
    FROM VITAMIN
    WHERE URL IS NOT NULL AND URL <> ''
    GROUP BY URL
)
DELETE FROM VITAMIN
WHERE rowid NOT IN (SELECT keep_id FROM to_keep)
AND URL IN (
    SELECT URL
    FROM VITAMIN
    WHERE URL IS NOT NULL AND URL <> ''
    GROUP BY URL
    HAVING COUNT(*) > 1
);
"""
conn.execute(delete_duplicates_sql)
conn.commit()
rows = cursor.fetchall()
for row in rows : 
    print(row)
