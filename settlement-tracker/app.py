# app.py - 维鲸运营系统数据库与工具函数
# 完整文件：请直接覆盖 settlement-tracker/app.py
import sqlite3
import pandas as pd
from datetime import datetime
import re

# 店铺列表（加上"汇总"）
SHOP_LIST = ["云企", "鲸画", "知己知彼", "鼎银", "德勤", "淘小铺", "维鲸", "点小饿", "扶风", "汇总"]

# 初始化数据库
def init_database():
    conn = sqlite3.connect('settlement_system.db')
    cursor = conn.cursor()
    
    # 店铺表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS shops (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        shop_name TEXT UNIQUE
    )
    ''')
    
    # 插入店铺数据
    for shop in SHOP_LIST:
        try:
            cursor.execute("INSERT OR IGNORE INTO shops (shop_name) VALUES (?)", (shop,))
        except:
            pass
    
    # 售后问题表（根据违规ID）
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS after_sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        shop_id INTEGER,
        violation_id TEXT,
        sku_id TEXT,
        product_name TEXT,
        settlement_amount REAL,
        currency TEXT,
        account_time TIMESTAMP,
        settlement_date DATE,
        upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (shop_id) REFERENCES shops (id)
    )
    ''')
    
    # 交易结算表（根据备货单号）
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS transaction_settlements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        shop_id INTEGER,
        order_id TEXT,
        after_sale_id TEXT,
        stock_order_id TEXT,
        stock_order_type TEXT,
        sku_id TEXT,
        sku_code TEXT,
        product_name TEXT,
        sku_attribute TEXT,
        quantity INTEGER,
        coupon_amount REAL,
        store_coupon_amount REAL,
        declared_discount REAL,
        transaction_type TEXT,
        amount REAL,
        currency TEXT,
        account_time TIMESTAMP,
        settlement_date DATE,
        upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (shop_id) REFERENCES shops (id)
    )
    ''')
    
    # 日汇总表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_summary (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        shop_id INTEGER,
        settlement_date DATE,
        total_sales REAL DEFAULT 0,
        total_refunds REAL DEFAULT 0,
        total_subsidies REAL DEFAULT 0,
        total_after_sales REAL DEFAULT 0,
        upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (shop_id) REFERENCES shops (id),
        UNIQUE(shop_id, settlement_date)
    )
    ''')
    
    # 发货明细表（新增）
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS shipping_details (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        shop_id INTEGER,
        spu_id TEXT,
        skc_id TEXT,
        sku_id TEXT,
        product_name TEXT,
        sku_attribute TEXT,
        stock_order_id TEXT,
        quantity INTEGER,
        unit_price REAL DEFAULT 0,
        total_amount REAL DEFAULT 0,
        shipping_date DATE,
        upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (shop_id) REFERENCES shops (id),
        UNIQUE(shop_id, stock_order_id, sku_id)
    )
    ''')
    
    # 商品价格表（新增）
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS product_prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        shop_id INTEGER,
        spu_id TEXT,
        skc_id TEXT,
        sku_id TEXT,
        product_name TEXT,
        sku_attribute TEXT,
        unit_price REAL DEFAULT 0,
        cost_price REAL DEFAULT 0,
        update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (shop_id) REFERENCES shops (id),
        UNIQUE(shop_id, spu_id, sku_attribute)
    )
    ''')
    
    conn.commit()
    conn.close()
    print("数据库初始化完成！")

# 从备货单号解析日期（WB251016xxxx → 2025-10-16）
def parse_date_from_stock_id(stock_order_id):
    if not stock_order_id or not isinstance(stock_order_id, str):
        return None
    
    # 多种格式匹配，优先匹配 WB + YYMMDD
    patterns = [
        r'WB(\d{6})',   # WB251016
        r'WB-(\d{6})',  # WB-251016
        r'WB_(\d{6})',  # WB_251016
    ]
    for pattern in patterns:
        match = re.search(pattern, stock_order_id)
        if match:
            date_str = match.group(1)
            try:
                if len(date_str) == 6:
                    year = int("20" + date_str[:2])
                    month = int(date_str[2:4])
                    day = int(date_str[4:6])
                    if 1 <= month <= 12 and 1 <= day <= 31:
                        return f"{year:04d}-{month:02d}-{day:02d}"
            except:
                continue
    
    # 备用：直接匹配 YYYY-MM-DD
    match2 = re.search(r'(\d{4})-(\d{2})-(\d{2})', str(stock_order_id))
    if match2:
        return f"{match2.group(1)}-{match2.group(2)}-{match2.group(3)}"
    return None

# 获取店铺ID
def get_shop_id(shop_name):
    conn = sqlite3.connect('settlement_system.db')
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM shops WHERE shop_name = ?", (shop_name,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

# 插入售后问题数据
def insert_after_sales(df, shop_name):
    shop_id = get_shop_id(shop_name)
    if not shop_id:
        print(f"店铺 '{shop_name}' 不存在")
        return 0, 0
    
    conn = sqlite3.connect('settlement_system.db')
    inserted_count = 0
    skipped_count = 0
    
    for _, row in df.iterrows():
        try:
            amount = row.get('赔付金额', 0)
            if pd.isna(amount):
                amount = 0
            
            account_time = str(row.get('账务时间', '')).strip()
            settlement_date = None
            if account_time and len(account_time) >= 10:
                settlement_date = account_time[:10]
            
            violation_id = str(row.get('违规ID', '')).strip()
            sku_id = str(row.get('SKU ID', '')).strip()
            
            if after_sale_exists(shop_id, violation_id, sku_id, account_time, settlement_date):
                skipped_count += 1
                continue
            
            conn.execute('''
            INSERT INTO after_sales 
            (shop_id, violation_id, sku_id, product_name, settlement_amount, currency, account_time, settlement_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                shop_id,
                violation_id,
                sku_id,
                str(row.get('货品名称', '')).strip(),
                float(amount),
                str(row.get('币种', 'CNY')).strip(),
                account_time,
                settlement_date
            ))
            inserted_count += 1
            
        except Exception as e:
            print(f"插入售后数据出错: {e}")
    
    conn.commit()
    conn.close()
    return inserted_count, skipped_count

# 插入交易结算数据
def insert_transactions(df, shop_name):
    shop_id = get_shop_id(shop_name)
    if not shop_id:
        print(f"店铺 '{shop_name}' 不存在")
        return 0, 0
    
    conn = sqlite3.connect('settlement_system.db')
    inserted_count = 0
    skipped_count = 0
    
    for _, row in df.iterrows():
        try:
            stock_order_id = str(row.get('备货单号', '')).strip()
            settlement_date = parse_date_from_stock_id(stock_order_id)
            
            account_time = str(row.get('账务时间', ''))
            if not settlement_date:
                if account_time and len(account_time) >= 10:
                    settlement_date = account_time[:10]
            
            quantity = row.get('数量', 1)
            if pd.isna(quantity) or quantity == '/':
                quantity = 1
            else:
                try:
                    quantity = int(float(str(quantity)))
                except:
                    quantity = 1
            
            amount = row.get('金额', 0)
            if pd.isna(amount) or amount == '/':
                amount = 0
            else:
                try:
                    amount = float(str(amount))
                except:
                    amount = 0
            
            sku_id = str(row.get('SKU ID', '')).strip()
            transaction_type = str(row.get('交易类型', '销售回款')).strip()
            
            if transaction_exists(shop_id, sku_id, account_time, transaction_type, settlement_date):
                skipped_count += 1
                continue
            
            def parse_amount(value):
                if pd.isna(value) or value == '/' or value == '':
                    return 0
                try:
                    return float(str(value))
                except:
                    return 0
            
            conn.execute('''
            INSERT INTO transaction_settlements 
            (shop_id, order_id, after_sale_id, stock_order_id, stock_order_type, sku_id, sku_code, 
             product_name, sku_attribute, quantity, coupon_amount, store_coupon_amount, 
             declared_discount, transaction_type, amount, currency, account_time, settlement_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                shop_id,
                str(row.get('订单编号', '')).strip(),
                str(row.get('售后单号', '')).strip(),
                stock_order_id,
                str(row.get('备货单类型', '定制品')).strip(),
                sku_id,
                str(row.get('SKU货号', '')).strip(),
                str(row.get('货品名称', '')).strip(),
                str(row.get('SKU属性', '')).strip(),
                quantity,
                parse_amount(row.get('单品券金额', 0)),
                parse_amount(row.get('店铺满减券金额', 0)),
                parse_amount(row.get('申报价格折扣金额', 0)),
                transaction_type,
                amount,
                str(row.get('币种', 'CNY')).strip(),
                account_time,
                settlement_date
            ))
            inserted_count += 1
            
        except Exception as e:
            print(f"插入交易数据出错: {e}")
    
    conn.commit()
    conn.close()
    return inserted_count, skipped_count

# 插入发货明细数据
def insert_shipping_details(df, shop_name):
    shop_id = get_shop_id(shop_name)
    if not shop_id:
        print(f"店铺 '{shop_name}' 不存在")
        return 0, 0
    
    conn = _connect()
    inserted_count = 0
    skipped_count = 0
    
    for _, row in df.iterrows():
        try:
            stock_order = str(row.get('备货单', '')).strip()
            stock_order_id = None
            quantity = 1
            
            # 解析备货单号和数量
            if '，' in stock_order:
                parts = stock_order.split('，')
                if len(parts) >= 2:
                    stock_order_id = parts[0].strip()
                    quantity_str = parts[1].replace('件', '').strip()
                    try:
                        quantity = int(quantity_str)
                    except:
                        quantity = 1
            else:
                stock_order_id = stock_order
            
            shipping_date = parse_date_from_stock_id(stock_order_id) if stock_order_id else None
            
            spu_id = str(row.get('商品SPU ID', '')).strip()
            skc_id = str(row.get('商品SKC ID', '')).strip()
            if not skc_id:
                skc_id = str(row.get('SKC ID', '')).strip()
            sku_id = str(row.get('商品SKU ID', '')).strip()
            product_name = str(row.get('商品名称', '')).strip()
            sku_attribute = str(row.get('商品属性集', '')).strip()
            
            if shipping_detail_exists(shop_id, stock_order_id, sku_id):
                skipped_count += 1
                continue
            
            # **关键修改：检查并插入 product_prices（确保商品存在）**
            cursor = conn.cursor()
            cursor.execute('''
            SELECT unit_price, cost_price FROM product_prices 
            WHERE shop_id = ? AND spu_id = ? AND sku_attribute = ?
            ''', (shop_id, spu_id, sku_attribute))
            
            price_result = cursor.fetchone()
            
            if price_result:
                # 如果商品已存在，使用商品表中的价格
                unit_price = price_result[0] or 0
                cost_price = price_result[1] or 0
            else:
                # 如果商品不存在，创建新的商品记录
                unit_price = 0  # 默认价格
                cost_price = 0  # 默认成本价
                
                # 检查是否传入价格信息（从其他列获取）
                unit_price_col = row.get('申报价格', row.get('单价', row.get('价格', 0)))
                if unit_price_col:
                    try:
                        unit_price = float(unit_price_col)
                    except:
                        unit_price = 0
                
                cost_price_col = row.get('成本单价', row.get('成本价', row.get('成本', 0)))
                if cost_price_col:
                    try:
                        cost_price = float(cost_price_col)
                    except:
                        cost_price = 0
                
                # 插入新的商品记录
                cursor.execute('''
                INSERT OR IGNORE INTO product_prices 
                (shop_id, spu_id, skc_id, sku_id, product_name, sku_attribute, unit_price, cost_price)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    shop_id,
                    spu_id,
                    skc_id,
                    sku_id,
                    product_name,
                    sku_attribute,
                    unit_price,
                    cost_price
                ))
                print(f"✅ 自动创建商品记录: {product_name} ({sku_attribute})")
            
            total_amount = unit_price * quantity
            
            # 插入发货明细
            cursor.execute('''
            INSERT INTO shipping_details 
            (shop_id, spu_id, skc_id, sku_id, product_name, sku_attribute, 
             stock_order_id, quantity, unit_price, total_amount, shipping_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                shop_id,
                spu_id,
                skc_id,
                sku_id,
                product_name,
                sku_attribute,
                stock_order_id,
                quantity,
                unit_price,
                total_amount,
                shipping_date
            ))
            inserted_count += 1
            
        except Exception as e:
            print(f"插入发货数据出错: {e}, 行数据: {row.to_dict()}")
    
    conn.commit()
    conn.close()
    print(f"✅ 导入完成: 新增 {inserted_count} 条发货记录")
    return inserted_count, skipped_count

# 更新日汇总数据
def update_daily_summary(shop_name):
    shop_id = get_shop_id(shop_name)
    if not shop_id:
        return
    
    conn = sqlite3.connect('settlement_system.db')
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT DISTINCT settlement_date 
    FROM transaction_settlements 
    WHERE shop_id = ? AND settlement_date IS NOT NULL
    UNION
    SELECT DISTINCT settlement_date 
    FROM after_sales 
    WHERE shop_id = ? AND settlement_date IS NOT NULL
    ''', (shop_id, shop_id))
    
    dates = [row[0] for row in cursor.fetchall()]
    
    for date in dates:
        if not date:
            continue
            
        cursor.execute('''
        SELECT COALESCE(SUM(amount), 0) 
        FROM transaction_settlements 
        WHERE shop_id = ? AND settlement_date = ? AND transaction_type = '销售回款'
        ''', (shop_id, date))
        total_sales = cursor.fetchone()[0]
        
        cursor.execute('''
        SELECT COALESCE(SUM(amount), 0) 
        FROM transaction_settlements 
        WHERE shop_id = ? AND settlement_date = ? AND transaction_type = '销售冲回'
        ''', (shop_id, date))
        total_refunds = cursor.fetchone()[0]
        
        cursor.execute('''
        SELECT COALESCE(SUM(amount), 0) 
        FROM transaction_settlements 
        WHERE shop_id = ? AND settlement_date = ? AND transaction_type = '非商责补贴'
        ''', (shop_id, date))
        total_subsidies = cursor.fetchone()[0]
        
        cursor.execute('''
        SELECT COALESCE(SUM(settlement_amount), 0) 
        FROM after_sales 
        WHERE shop_id = ? AND settlement_date = ?
        ''', (shop_id, date))
        total_after_sales = cursor.fetchone()[0]
        
        cursor.execute('''
        INSERT OR REPLACE INTO daily_summary 
        (shop_id, settlement_date, total_sales, total_refunds, total_subsidies, total_after_sales)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (shop_id, date, total_sales, total_refunds, total_subsidies, total_after_sales))
    
    conn.commit()
    conn.close()

# 计算所有店铺的汇总数据
def update_all_shops_summary():
    conn = sqlite3.connect('settlement_system.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, shop_name FROM shops WHERE shop_name != '汇总'")
    shops = cursor.fetchall()
    
    cursor.execute('''
    SELECT DISTINCT settlement_date 
    FROM daily_summary 
    WHERE settlement_date IS NOT NULL
    ORDER BY settlement_date
    ''')
    
    all_dates = [row[0] for row in cursor.fetchall()]
    
    for date in all_dates:
        if not date:
            continue
            
        total_sales = 0
        total_refunds = 0
        total_subsidies = 0
        total_after_sales = 0
        
        for shop_id, shop_name in shops:
            cursor.execute('''
            SELECT total_sales, total_refunds, total_subsidies, total_after_sales
            FROM daily_summary 
            WHERE shop_id = ? AND settlement_date = ?
            ''', (shop_id, date))
            result = cursor.fetchone()
            
            if result:
                total_sales += result[0]
                total_refunds += result[1]
                total_subsidies += result[2]
                total_after_sales += result[3]
        
        cursor.execute("SELECT id FROM shops WHERE shop_name = '汇总'")
        summary_shop_id_result = cursor.fetchone()
        
        if summary_shop_id_result:
            summary_shop_id = summary_shop_id_result[0]
            cursor.execute('''
            INSERT OR REPLACE INTO daily_summary 
            (shop_id, settlement_date, total_sales, total_refunds, total_subsidies, total_after_sales)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (summary_shop_id, date, total_sales, total_refunds, total_subsidies, total_after_sales))
    
    conn.commit()
    conn.close()

# 获取日汇总数据
def get_daily_summary(shop_name, date):
    shop_id = get_shop_id(shop_name)
    if not shop_id:
        return None
    
    conn = sqlite3.connect('settlement_system.db')
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT total_sales, total_refunds, total_subsidies, total_after_sales
    FROM daily_summary 
    WHERE shop_id = ? AND settlement_date = ?
    ''', (shop_id, date))
    
    result = cursor.fetchone()
    conn.close()
    
    if result:
        return {
            'total_sales': result[0],
            'total_refunds': result[1],
            'total_subsidies': result[2],
            'total_after_sales': result[3]
        }
    return None

# 获取月度汇总数据
def get_monthly_summary(shop_name, year, month):
    shop_id = get_shop_id(shop_name)
    if not shop_id:
        return None
    
    month_str = f"{year:04d}-{month:02d}"
    
    conn = sqlite3.connect('settlement_system.db')
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT 
        settlement_date,
        SUM(CASE WHEN transaction_type = '销售回款' THEN amount ELSE 0 END) as daily_sales,
        SUM(CASE WHEN transaction_type = '销售冲回' THEN amount ELSE 0 END) as daily_refunds,
        SUM(CASE WHEN transaction_type = '非商责补贴' THEN amount ELSE 0 END) as daily_subsidies,
        COUNT(CASE WHEN transaction_type = '销售回款' THEN 1 END) as sales_count,
        COUNT(CASE WHEN transaction_type = '销售冲回' THEN 1 END) as refunds_count,
        COUNT(CASE WHEN transaction_type = '非商责补贴' THEN 1 END) as subsidies_count
    FROM transaction_settlements 
    WHERE shop_id = ? AND strftime('%Y-%m', settlement_date) = ?
    GROUP BY settlement_date
    ORDER BY settlement_date
    ''', (shop_id, month_str))
    
    daily_transactions = cursor.fetchall()
    
    cursor.execute('''
    SELECT 
        settlement_date,
        SUM(settlement_amount) as daily_after_sales,
        COUNT(*) as after_sales_count
    FROM after_sales 
    WHERE shop_id = ? AND strftime('%Y-%m', settlement_date) = ?
    GROUP BY settlement_date
    ORDER BY settlement_date
    ''', (shop_id, month_str))
    
    daily_after_sales = cursor.fetchall()
    
    daily_data = {}
    
    for row in daily_transactions:
        date = row[0]
        daily_data[date] = {
            'date': date,
            'sales': row[1] or 0,
            'refunds': row[2] or 0,
            'subsidies': row[3] or 0,
            'sales_count': row[4] or 0,
            'refunds_count': row[5] or 0,
            'subsidies_count': row[6] or 0,
            'after_sales': 0,
            'after_sales_count': 0
        }
    
    for row in daily_after_sales:
        date = row[0]
        if date in daily_data:
            daily_data[date]['after_sales'] = row[1] or 0
            daily_data[date]['after_sales_count'] = row[2] or 0
        else:
            daily_data[date] = {
                'date': date,
                'sales': 0,
                'refunds': 0,
                'subsidies': 0,
                'sales_count': 0,
                'refunds_count': 0,
                'subsidies_count': 0,
                'after_sales': row[1] or 0,
                'after_sales_count': row[2] or 0
            }
    
    monthly_sales = sum(row[1] or 0 for row in daily_transactions)
    monthly_refunds = sum(row[2] or 0 for row in daily_transactions)
    monthly_subsidies = sum(row[3] or 0 for row in daily_transactions)
    monthly_after_sales = sum(row[1] or 0 for row in daily_after_sales)
    
    total_sales_count = sum(row[4] for row in daily_transactions)
    total_refunds_count = sum(row[5] for row in daily_transactions)
    total_subsidies_count = sum(row[6] for row in daily_transactions)
    total_after_sales_count = sum(row[2] for row in daily_after_sales)
    
    conn.close()
    
    daily_list = sorted(daily_data.values(), key=lambda x: x['date'])
    
    return {
        'month': month_str,
        'daily_data': daily_list,
        'monthly_totals': {
            'sales': monthly_sales,
            'refunds': monthly_refunds,
            'subsidies': monthly_subsidies,
            'after_sales': monthly_after_sales,
            'total_sales_count': total_sales_count,
            'total_refunds_count': total_refunds_count,
            'total_subsidies_count': total_subsidies_count,
            'total_after_sales_count': total_after_sales_count,
            'total_transactions': total_sales_count + total_refunds_count + total_subsidies_count
        }
    }

# 获取指定日期的所有店铺汇总
def get_all_shops_summary(date):
    conn = sqlite3.connect('settlement_system.db')
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT s.shop_name, d.total_sales, d.total_refunds, d.total_subsidies, d.total_after_sales
    FROM shops s
    LEFT JOIN daily_summary d ON s.id = d.shop_id AND d.settlement_date = ?
    WHERE s.shop_name != '汇总'
    ORDER BY 
        CASE s.shop_name
            WHEN '云企' THEN 1
            WHEN '鲸画' THEN 2
            WHEN '知己知彼' THEN 3
            WHEN '鼎银' THEN 4
            WHEN '德勤' THEN 5
            WHEN '淘小铺' THEN 6
            WHEN '维鲸' THEN 7
            WHEN '点小饿' THEN 8
            WHEN '扶风' THEN 9
            ELSE 10
        END
    ''', (date,))
    
    results = cursor.fetchall()
    
    cursor.execute('''
    SELECT total_sales, total_refunds, total_subsidies, total_after_sales
    FROM shops s
    JOIN daily_summary d ON s.id = d.shop_id
    WHERE s.shop_name = '汇总' AND d.settlement_date = ?
    ''', (date,))
    
    summary_result = cursor.fetchone()
    
    conn.close()
    
    shops_data = []
    for row in results:
        shops_data.append({
            'shop_name': row[0],
            'total_sales': row[1] if row[1] is not None else 0,
            'total_refunds': row[2] if row[2] is not None else 0,
            'total_subsidies': row[3] if row[3] is not None else 0,
            'total_after_sales': row[4] if row[4] is not None else 0
        })
    
    return {
        'shops': shops_data,
        'summary': {
            'total_sales': summary_result[0] if summary_result else 0,
            'total_refunds': summary_result[1] if summary_result else 0,
            'total_subsidies': summary_result[2] if summary_result else 0,
            'total_after_sales': summary_result[3] if summary_result else 0
        }
    }

# 搜索订单
def search_orders(shop_name=None, stock_order_id=None, order_id=None, date=None):
    conn = sqlite3.connect('settlement_system.db')
    
    query = '''
    SELECT t.*, s.shop_name
    FROM transaction_settlements t
    JOIN shops s ON t.shop_id = s.id
    WHERE 1=1
    '''
    params = []
    
    if shop_name and shop_name != "所有店铺":
        query += " AND s.shop_name = ?"
        params.append(shop_name)
    
    if stock_order_id:
        query += " AND t.stock_order_id LIKE ?"
        params.append(f"%{stock_order_id}%")
    
    if order_id:
        query += " AND t.order_id LIKE ?"
        params.append(f"%{order_id}%")
    
    if date:
        query += " AND t.settlement_date = ?"
        params.append(date)
    
    query += " ORDER BY t.settlement_date DESC, t.account_time DESC"
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

# 搜索售后问题
def search_after_sales(shop_name=None, violation_id=None, date=None):
    conn = sqlite3.connect('settlement_system.db')
    
    query = '''
    SELECT a.*, s.shop_name
    FROM after_sales a
    JOIN shops s ON a.shop_id = s.id
    WHERE 1=1
    '''
    params = []
    
    if shop_name and shop_name != "所有店铺":
        query += " AND s.shop_name = ?"
        params.append(shop_name)
    
    if violation_id:
        query += " AND a.violation_id LIKE ?"
        params.append(f"%{violation_id}%")
    
    if date:
        query += " AND a.settlement_date = ?"
        params.append(date)
    
    query += " ORDER BY a.settlement_date DESC, a.account_time DESC"
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

# 搜索发货明细
def search_shipping_details(shop_name=None, spu_id=None, sku_id=None, stock_order_id=None, start_date=None, end_date=None):
    conn = sqlite3.connect('settlement_system.db')
    
    query = '''
    SELECT s.*, sh.shop_name
    FROM shipping_details s
    JOIN shops sh ON s.shop_id = sh.id
    WHERE 1=1
    '''
    params = []
    
    if shop_name and shop_name != "所有店铺":
        query += " AND sh.shop_name = ?"
        params.append(shop_name)
    
    if spu_id:
        query += " AND s.spu_id LIKE ?"
        params.append(f"%{spu_id}%")
    
    if sku_id:
        query += " AND s.sku_id LIKE ?"
        params.append(f"%{sku_id}%")
    
    if stock_order_id:
        query += " AND s.stock_order_id LIKE ?"
        params.append(f"%{stock_order_id}%")
    
    if start_date:
        query += " AND s.shipping_date >= ?"
        params.append(start_date)
    
    if end_date:
        query += " AND s.shipping_date <= ?"
        params.append(end_date)
    
    query += " ORDER BY s.shipping_date DESC, s.upload_date DESC"
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

# 获取商品列表
def get_products(shop_name=None, spu_id=None, product_name=None):
    """获取商品列表，返回每个规格的详细信息（不再按SPU聚合）"""
    conn = _connect()
    
    query = '''
    SELECT 
        p.id,
        p.spu_id,
        p.skc_id,
        p.sku_id,
        p.product_name,
        p.sku_attribute,
        p.unit_price,
        p.cost_price,
        p.update_date,
        COALESCE(SUM(sd.quantity), 0) AS total_sold,
        COALESCE(SUM(sd.total_amount), 0) AS total_sales_amount,
        sh.shop_name
    FROM product_prices p
    JOIN shops sh ON p.shop_id = sh.id
    LEFT JOIN shipping_details sd ON p.shop_id = sd.shop_id 
        AND p.spu_id = sd.spu_id 
        AND p.sku_attribute = sd.sku_attribute
    WHERE 1=1
    '''
    params = []
    
    if shop_name and shop_name != "所有店铺":
        query += " AND sh.shop_name = ?"
        params.append(shop_name)
    
    if spu_id:
        query += " AND p.spu_id LIKE ?"
        params.append(f"%{spu_id}%")
    
    if product_name:
        query += " AND p.product_name LIKE ?"
        params.append(f"%{product_name}%")
    
    query += " GROUP BY p.id, p.shop_id, p.spu_id, p.sku_attribute"
    query += " ORDER BY sh.shop_name, p.spu_id, p.sku_attribute"
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

# 更新商品价格（按 SPU/可选规格）：同时同步发货明细中的单价、总金额，以及商品名称和规格
def update_product_price(shop_name, spu_id, sku_attribute, unit_price, cost_price, product_name=None):
    shop_id = get_shop_id(shop_name)
    if not shop_id:
        return False
    
    conn = _connect()
    try:
        cursor = conn.cursor()
        
        if sku_attribute is None or sku_attribute == '':
            # 更新所有该 SPU 的价格与名称
            print(f"📝 正在更新 SPU {spu_id} 的所有规格...")
            
            cursor.execute('''
            UPDATE product_prices
            SET unit_price = ?, cost_price = ?, product_name = ?, update_date = CURRENT_TIMESTAMP
            WHERE shop_id = ? AND spu_id = ?
            ''', (unit_price, cost_price, product_name if product_name is not None else '', shop_id, spu_id))
            
            # **同步更新发货明细中所有相关记录**
            cursor.execute('''
            UPDATE shipping_details
            SET unit_price = ?, 
                total_amount = quantity * ?, 
                product_name = ?
            WHERE shop_id = ? AND spu_id = ?
            ''', (unit_price, unit_price, product_name if product_name is not None else '', shop_id, spu_id))
            
            # 获取更新数量
            cursor.execute('''
            SELECT COUNT(*) FROM shipping_details 
            WHERE shop_id = ? AND spu_id = ?
            ''', (shop_id, spu_id))
            updated_count = cursor.fetchone()[0]
            print(f"✅ 已同步更新 {updated_count} 条发货明细记录")
            
        else:
            # 仅更新特定 sku_attribute
            print(f"📝 正在更新 SPU {spu_id} 的规格 {sku_attribute}...")
            
            cursor.execute('''
            UPDATE product_prices
            SET unit_price = ?, cost_price = ?, product_name = ?, update_date = CURRENT_TIMESTAMP
            WHERE shop_id = ? AND spu_id = ? AND sku_attribute = ?
            ''', (unit_price, cost_price, product_name if product_name is not None else '', shop_id, spu_id, sku_attribute))
            
            # **同步更新发货明细中所有相关记录**
            cursor.execute('''
            UPDATE shipping_details
            SET unit_price = ?, 
                total_amount = quantity * ?, 
                product_name = ?
            WHERE shop_id = ? AND spu_id = ? AND sku_attribute = ?
            ''', (unit_price, unit_price, product_name if product_name is not None else '', shop_id, spu_id, sku_attribute))
            
            # 获取更新数量
            cursor.execute('''
            SELECT COUNT(*) FROM shipping_details 
            WHERE shop_id = ? AND spu_id = ? AND sku_attribute = ?
            ''', (shop_id, spu_id, sku_attribute))
            updated_count = cursor.fetchone()[0]
            print(f"✅ 已同步更新 {updated_count} 条发货明细记录")
        
        conn.commit()
        return True
    except Exception as e:
        print(f"更新商品价格出错: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

# 获取销售分析
def get_sales_analysis(shop_name, year, month):
    shop_id = get_shop_id(shop_name)
    if not shop_id:
        return pd.DataFrame()
    
    month_str = f"{year:04d}-{month:02d}"
    
    conn = sqlite3.connect('settlement_system.db')
    
    query = '''
    SELECT 
        p.spu_id,
        p.skc_id,
        p.sku_id,
        p.product_name,
        p.sku_attribute,
        p.unit_price,
        p.cost_price,
        COALESCE(SUM(CASE WHEN strftime('%Y-%m', s.shipping_date) = ? THEN s.quantity ELSE 0 END), 0) as total_quantity,
        COALESCE(SUM(CASE WHEN strftime('%Y-%m', s.shipping_date) = ? THEN s.total_amount ELSE 0 END), 0) as total_amount,
        COALESCE(SUM(CASE WHEN strftime('%Y-%m', s.shipping_date) = ? THEN s.quantity * p.cost_price ELSE 0 END), 0) as total_cost,
        COUNT(DISTINCT CASE WHEN strftime('%Y-%m', s.shipping_date) = ? THEN s.stock_order_id END) as order_count
    FROM product_prices p
    LEFT JOIN shipping_details s ON p.shop_id = s.shop_id AND p.spu_id = s.spu_id AND p.sku_attribute = s.sku_attribute
    WHERE p.shop_id = ?
    GROUP BY p.spu_id, p.sku_attribute
    HAVING total_quantity > 0 OR total_amount > 0
    ORDER BY total_amount DESC, total_quantity DESC
    '''
    
    df = pd.read_sql_query(query, conn, params=[month_str, month_str, month_str, month_str, shop_id])
    conn.close()
    return df

# 发货与结款对比分析
def compare_shipping_settlement(shop_name, start_date=None, end_date=None):
    shop_id = get_shop_id(shop_name)
    if not shop_id:
        return pd.DataFrame()
    
    conn = sqlite3.connect('settlement_system.db')
    
    shipping_query = '''
    SELECT 
        s.stock_order_id,
        s.spu_id,
        s.skc_id,
        s.sku_id,
        s.product_name,
        s.sku_attribute,
        SUM(s.quantity) as shipping_quantity,
        SUM(s.total_amount) as shipping_amount,
        s.shipping_date
    FROM shipping_details s
    WHERE s.shop_id = ?
    '''
    shipping_params = [shop_id]
    
    if start_date:
        shipping_query += " AND s.shipping_date >= ?"
        shipping_params.append(start_date)
    if end_date:
        shipping_query += " AND s.shipping_date <= ?"
        shipping_params.append(end_date)
    
    shipping_query += " GROUP BY s.stock_order_id, s.spu_id, s.sku_attribute"
    
    shipping_df = pd.read_sql_query(shipping_query, conn, params=shipping_params)
    
    if len(shipping_df) == 0:
        conn.close()
        return pd.DataFrame()
    
    stock_order_ids = shipping_df['stock_order_id'].dropna().unique()
    if len(stock_order_ids) == 0:
        conn.close()
        return pd.DataFrame()
    
    placeholders = ','.join(['?' for _ in stock_order_ids])
    settlement_query = f'''
    SELECT 
        stock_order_id,
        SUM(CASE WHEN transaction_type = '销售回款' THEN amount ELSE 0 END) as settlement_amount,
        SUM(CASE WHEN transaction_type = '销售回款' THEN quantity ELSE 0 END) as settlement_quantity,
        COUNT(CASE WHEN transaction_type = '销售回款' THEN 1 END) as settlement_count
    FROM transaction_settlements 
    WHERE shop_id = ? AND stock_order_id IN ({placeholders})
    GROUP BY stock_order_id
    '''
    
    settlement_params = [shop_id] + list(stock_order_ids)
    settlement_df = pd.read_sql_query(settlement_query, conn, params=settlement_params)
    
    conn.close()
    
    if len(settlement_df) > 0:
        result_df = pd.merge(
            shipping_df, 
            settlement_df, 
            on='stock_order_id', 
            how='left'
        )
        result_df['settlement_amount'] = result_df['settlement_amount'].fillna(0)
        result_df['settlement_quantity'] = result_df['settlement_quantity'].fillna(0)
        result_df['settlement_count'] = result_df['settlement_count'].fillna(0)
    else:
        result_df = shipping_df.copy()
        result_df['settlement_amount'] = 0
        result_df['settlement_quantity'] = 0
        result_df['settlement_count'] = 0
    
    result_df['settlement_rate'] = result_df.apply(
        lambda row: (row['settlement_amount'] / row['shipping_amount'] * 100) if row['shipping_amount'] > 0 else 0,
        axis=1
    )
    
    result_df['shipping_date'] = pd.to_datetime(result_df['shipping_date']).dt.strftime('%Y-%m-%d')
    
    return result_df

# 清除所有数据（用于测试）
def clear_all_data():
    conn = sqlite3.connect('settlement_system.db')
    cursor = conn.cursor()
    
    cursor.execute("DELETE FROM after_sales")
    cursor.execute("DELETE FROM transaction_settlements")
    cursor.execute("DELETE FROM daily_summary")
    cursor.execute("DELETE FROM shipping_details")
    cursor.execute("DELETE FROM product_prices")
    
    conn.commit()
    conn.close()
    print("✅ 所有数据已清除！")

# 获取所有日期
def get_all_dates():
    conn = sqlite3.connect('settlement_system.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT DISTINCT settlement_date FROM transaction_settlements WHERE settlement_date IS NOT NULL ORDER BY settlement_date DESC")
    transaction_dates = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT DISTINCT settlement_date FROM after_sales WHERE settlement_date IS NOT NULL ORDER BY settlement_date DESC")
    after_sales_dates = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT DISTINCT shipping_date FROM shipping_details WHERE shipping_date IS NOT NULL ORDER BY shipping_date DESC")
    shipping_dates = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    
    return {
        'transaction_dates': transaction_dates,
        'after_sales_dates': after_sales_dates,
        'shipping_dates': shipping_dates
    }

# 检查记录是否已存在的工具函数
def transaction_exists(shop_id, sku_id, account_time, transaction_type, settlement_date):
    conn = sqlite3.connect('settlement_system.db')
    cursor = conn.cursor()
    cursor.execute('''
    SELECT COUNT(*) FROM transaction_settlements 
    WHERE shop_id = ? AND sku_id = ? AND account_time = ? 
    AND transaction_type = ? AND settlement_date = ?
    ''', (shop_id, sku_id, account_time, transaction_type, settlement_date))
    count = cursor.fetchone()[0]
    conn.close()
    return count > 0

def after_sale_exists(shop_id, violation_id, sku_id, account_time, settlement_date):
    conn = sqlite3.connect('settlement_system.db')
    cursor = conn.cursor()
    cursor.execute('''
    SELECT COUNT(*) FROM after_sales 
    WHERE shop_id = ? AND violation_id = ? AND sku_id = ? 
    AND account_time = ? AND settlement_date = ?
    ''', (shop_id, violation_id, sku_id, account_time, settlement_date))
    count = cursor.fetchone()[0]
    conn.close()
    return count > 0

def shipping_detail_exists(shop_id, stock_order_id, sku_id):
    if not stock_order_id:
        return False
    conn = sqlite3.connect('settlement_system.db')
    cursor = conn.cursor()
    cursor.execute('''
    SELECT COUNT(*) FROM shipping_details 
    WHERE shop_id = ? AND stock_order_id = ? AND sku_id = ?
    ''', (shop_id, stock_order_id, sku_id))
    count = cursor.fetchone()[0]
    conn.close()
    return count > 0

def debug_data():
    conn = sqlite3.connect('settlement_system.db')
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM after_sales")
    after_sales_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM transaction_settlements")
    transaction_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM daily_summary")
    summary_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM shipping_details")
    shipping_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM product_prices")
    product_count = cursor.fetchone()[0]
    cursor.execute("SELECT stock_order_id, settlement_date FROM transaction_settlements WHERE settlement_date IS NOT NULL LIMIT 10")
    samples = cursor.fetchall()
    conn.close()
    return {
        'after_sales_count': after_sales_count,
        'transaction_count': transaction_count,
        'summary_count': summary_count,
        'shipping_count': shipping_count,
        'product_count': product_count,
        'sample_data': samples
    }

if __name__ == "__main__":
    # 直接运行时初始化数据库，便于调试
    init_database()
    print("app.py 直接运行：数据库已初始化。")