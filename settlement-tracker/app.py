```python name=settlement-tracker/app.py url=https://github.com/leejme/wj/blob/main/settlement-tracker/app.py
# database.py - 维鲸运营系统数据库
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
        stock_order_id TEXT,  -- 备货单号
        stock_order_type TEXT,
        sku_id TEXT,
        sku_code TEXT,
        product_name TEXT,
        sku_attribute TEXT,
        quantity INTEGER,
        coupon_amount REAL,
        store_coupon_amount REAL,
        declared_discount REAL,
        transaction_type TEXT,  -- 销售回款、销售冲回、非商责补贴
        amount REAL,
        currency TEXT,
        account_time TIMESTAMP,
        settlement_date DATE,  -- 从备货单号解析出的日期
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
        total_sales REAL DEFAULT 0,      -- 销售回款总额
        total_refunds REAL DEFAULT 0,    -- 销售冲回总额
        total_subsidies REAL DEFAULT 0,  -- 非商责补贴总额
        total_after_sales REAL DEFAULT 0, -- 售后赔付总额
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
        spu_id TEXT,           -- 商品SPU ID
        skc_id TEXT,           -- 商品SKC ID（新增）
        sku_id TEXT,           -- 商品SKU ID
        product_name TEXT,     -- 商品名称
        sku_attribute TEXT,    -- 商品属性集（规格）
        stock_order_id TEXT,   -- 备货单号
        quantity INTEGER,      -- 件数
        unit_price REAL DEFAULT 0,     -- 单价（申报价格）
        total_amount REAL DEFAULT 0,   -- 总金额（申报额）
        shipping_date DATE,    -- 发货日期（从备货单号解析）
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
        spu_id TEXT,           -- 商品SPU ID
        skc_id TEXT,           -- 商品SKC ID（新增）
        sku_id TEXT,           -- 商品SKU ID
        product_name TEXT,     -- 商品名称
        sku_attribute TEXT,    -- 商品属性集（规格）
        unit_price REAL DEFAULT 0,     -- 销售单价 / 申报价格
        cost_price REAL DEFAULT 0,     -- 成本单价
        update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (shop_id) REFERENCES shops (id),
        UNIQUE(shop_id, spu_id, sku_attribute)
    )
    ''')
    
    conn.commit()
    conn.close()
    print("数据库初始化完成！")

# 从备货单号解析日期（WB2510162836467 → 2025-10-16）
def parse_date_from_stock_id(stock_order_id):
    if not stock_order_id or not isinstance(stock_order_id, str):
        return None
    
    # 查找日期部分（WB251016... → 251016）
    match = re.search(r'WB(\d{6})', stock_order_id)
    if match:
        date_str = match.group(1)
        try:
            # 解析日期格式：前2位年份，中间2位月份，后2位日期
            # 注意：25年应该解析为2025年
            year = int("20" + date_str[:2])  # 25 → 2025
            month = int(date_str[2:4])
            day = int(date_str[4:6])
            
            # 验证日期是否有效
            if 1 <= month <= 12 and 1 <= day <= 31:
                return f"{year:04d}-{month:02d}-{day:02d}"
            else:
                print(f"日期无效: {date_str}")
                return None
        except Exception as e:
            print(f"日期解析错误: {e}, 原始字符串: {stock_order_id}")
            return None
    else:
        # 允许其它格式后备选解析
        match2 = re.search(r'(\d{4})-(\d{2})-(\d{2})', stock_order_id)
        if match2:
            return f"{match2.group(1)}-{match2.group(2)}-{match2.group(3)}"
        print(f"未找到日期部分: {stock_order_id}")
        return None

# 获取店铺ID
def get_shop_id(shop_name):
    conn = sqlite3.connect('settlement_system.db')
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM shops WHERE shop_name = ?", (shop_name,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

# 插入售后问题数据（不需要结算日期参数）
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
            # 处理数值型数据
            amount = row.get('赔付金额', 0)
            if pd.isna(amount):
                amount = 0
            
            # 从账务时间解析日期
            account_time = str(row.get('账务时间', '')).strip()
            settlement_date = None
            if account_time and len(account_time) >= 10:
                settlement_date = account_time[:10]
            
            violation_id = str(row.get('违规ID', '')).strip()
            sku_id = str(row.get('SKU ID', '')).strip()
            
            # 检查是否已存在
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

# 插入交易结算数据（每个订单独立解析日期）
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
            # 处理关键字段
            stock_order_id = str(row.get('备货单号', '')).strip()
            
            # 从备货单号解析结算日期
            settlement_date = parse_date_from_stock_id(stock_order_id)
            
            # 如果解析失败，使用账务时间中的日期
            account_time = str(row.get('账务时间', ''))
            if not settlement_date:
                if account_time and len(account_time) >= 10:
                    settlement_date = account_time[:10]
            
            # 处理数量
            quantity = row.get('数量', 1)
            if pd.isna(quantity) or quantity == '/':
                quantity = 1
            else:
                try:
                    quantity = int(float(str(quantity)))
                except:
                    quantity = 1
            
            # 处理金额
            amount = row.get('金额', 0)
            if pd.isna(amount) or amount == '/':
                amount = 0
            else:
                try:
                    amount = float(str(amount))
                except:
                    amount = 0
            
            # 处理SKU ID
            sku_id = str(row.get('SKU ID', '')).strip()
            
            # 处理交易类型
            transaction_type = str(row.get('交易类型', '销售回款')).strip()
            
            # 检查是否已存在
            if transaction_exists(shop_id, sku_id, account_time, transaction_type, settlement_date):
                skipped_count += 1
                continue
            
            # 处理优惠金额
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

# 插入发货明细数据（新增）
def insert_shipping_details(df, shop_name):
    shop_id = get_shop_id(shop_name)
    if not shop_id:
        print(f"店铺 '{shop_name}' 不存在")
        return 0, 0
    
    conn = sqlite3.connect('settlement_system.db')
    
    inserted_count = 0
    skipped_count = 0
    
    for _, row in df.iterrows():
        try:
            # 解析备货单号和件数
            stock_order = str(row.get('备货单', '')).strip()
            stock_order_id = None
            quantity = 1
            
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
            
            # 从备货单号解析发货日期
            shipping_date = parse_date_from_stock_id(stock_order_id) if stock_order_id else None
            
            # 获取商品信息
            spu_id = str(row.get('商品SPU ID', '')).strip()
            skc_id = str(row.get('商品SKC ID', '')).strip()
            if not skc_id:
                skc_id = str(row.get('SKC ID', '')).strip()
            sku_id = str(row.get('商品SKU ID', '')).strip()
            product_name = str(row.get('商品名称', '')).strip()
            sku_attribute = str(row.get('商品属性集', '')).strip()
            
            # 检查是否已存在
            if shipping_detail_exists(shop_id, stock_order_id, sku_id):
                skipped_count += 1
                continue
            
            # 检查是否有该商品的价格信息
            unit_price = 0
            cursor = conn.cursor()
            cursor.execute('''
            SELECT unit_price FROM product_prices 
            WHERE shop_id = ? AND spu_id = ? AND sku_attribute = ?
            ''', (shop_id, spu_id, sku_attribute))
            price_result = cursor.fetchone()
            if price_result:
                unit_price = price_result[0]
            
            total_amount = unit_price * quantity
            
            # 插入发货明细
            conn.execute('''
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
            
            # 更新或插入商品价格表（如果没有的话）
            cursor.execute('''
            SELECT COUNT(*) FROM product_prices 
            WHERE shop_id = ? AND spu_id = ? AND sku_attribute = ?
            ''', (shop_id, spu_id, sku_attribute))
            exists = cursor.fetchone()[0]
            
            if exists == 0:
                conn.execute('''
                INSERT INTO product_prices 
                (shop_id, spu_id, skc_id, sku_id, product_name, sku_attribute, unit_price)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    shop_id,
                    spu_id,
                    skc_id,
                    sku_id,
                    product_name,
                    sku_attribute,
                    unit_price  # 初始为0，需要后续设置
                ))
            
        except Exception as e:
            print(f"插入发货数据出错: {e}, 行数据: {row.to_dict()}")
    
    conn.commit()
    conn.close()
    return inserted_count, skipped_count

# 更新日汇总数据
def update_daily_summary(shop_name):
    shop_id = get_shop_id(shop_name)
    if not shop_id:
        return
    
    conn = sqlite3.connect('settlement_system.db')
    cursor = conn.cursor()
    
    # 获取所有有交易的日期
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
            
        # 获取销售回款总额
        cursor.execute('''
        SELECT COALESCE(SUM(amount), 0) 
        FROM transaction_settlements 
        WHERE shop_id = ? AND settlement_date = ? AND transaction_type = '销售回款'
        ''', (shop_id, date))
        total_sales = cursor.fetchone()[0]
        
        # 获取销售冲回总额
        cursor.execute('''
        SELECT COALESCE(SUM(amount), 0) 
        FROM transaction_settlements 
        WHERE shop_id = ? AND settlement_date = ? AND transaction_type = '销售冲回'
        ''', (shop_id, date))
        total_refunds = cursor.fetchone()[0]
        
        # 获取非商责补贴总额
        cursor.execute('''
        SELECT COALESCE(SUM(amount), 0) 
        FROM transaction_settlements 
        WHERE shop_id = ? AND settlement_date = ? AND transaction_type = '非商责补贴'
        ''', (shop_id, date))
        total_subsidies = cursor.fetchone()[0]
        
        # 获取售后赔付总额
        cursor.execute('''
        SELECT COALESCE(SUM(settlement_amount), 0) 
        FROM after_sales 
        WHERE shop_id = ? AND settlement_date = ?
        ''', (shop_id, date))
        total_after_sales = cursor.fetchone()[0]
        
        # 更新或插入汇总数据
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
    
    # 获取所有店铺（除了"汇总"）
    cursor.execute("SELECT id, shop_name FROM shops WHERE shop_name != '汇总'")
    shops = cursor.fetchall()
    
    # 获取所有日期
    cursor.execute('''
    SELECT DISTINCT settlement_date 
    FROM daily_summary 
    WHERE settlement_date IS NOT NULL
    ORDER BY settlement_date
    ''')
    
    all_dates = [row[0] for row in cursor.fetchall()]
    
    # 更新每个日期的汇总数据
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
        
        # 获取"汇总"店铺的ID
        cursor.execute("SELECT id FROM shops WHERE shop_name = '汇总'")
        summary_shop_id = cursor.fetchone()
        
        if summary_shop_id:
            summary_shop_id = summary_shop_id[0]
            # 更新汇总数据
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

# 获取月度汇总数据（新功能）
def get_monthly_summary(shop_name, year, month):
    shop_id = get_shop_id(shop_name)
    if not shop_id:
        return None
    
    month_str = f"{year:04d}-{month:02d}"
    
    conn = sqlite3.connect('settlement_system.db')
    cursor = conn.cursor()
    
    # 获取当月每天的汇总
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
    
    # 获取当月售后汇总
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
    
    # 合并数据
    daily_data = {}
    
    # 处理交易数据
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
    
    # 处理售后数据
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
    
    # 计算月度总计
    monthly_sales = sum(row[1] or 0 for row in daily_transactions)
    monthly_refunds = sum(row[2] or 0 for row in daily_transactions)
    monthly_subsidies = sum(row[3] or 0 for row in daily_transactions)
    monthly_after_sales = sum(row[1] or 0 for row in daily_after_sales)
    
    # 计算各种单数的总计
    total_sales_count = sum(row[4] for row in daily_transactions)
    total_refunds_count = sum(row[5] for row in daily_transactions)
    total_subsidies_count = sum(row[6] for row in daily_transactions)
    total_after_sales_count = sum(row[2] for row in daily_after_sales)
    
    conn.close()
    
    # 转换为列表并排序
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
    
    # 获取汇总数据
    cursor.execute('''
    SELECT total_sales, total_refunds, total_subsidies, total_after_sales
    FROM shops s
    JOIN daily_summary d ON s.id = d.shop_id
    WHERE s.shop_name = '汇总' AND d.settlement_date = ?
    ''', (date,))
    
    summary_result = cursor.fetchone()
    
    conn.close()
    
    return {
        'shops': [{
            'shop_name': row[0],
            'total_sales': row[1] if row[1] is not None else 0,
            'total_refunds': row[2] if row[2] is not None else 0,
            'total_subsidies': row[3] if row[3] is not None else 0,
            'total_after_sales': row[4] if row[4] is not None else 0
        } for row in results],
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

# 搜索发货明细（新增）
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

# 获取商品列表（按 SPU 合并）
def get_products(shop_name=None, spu_id=None, product_name=None):
    conn = sqlite3.connect('settlement_system.db')
    
    # 聚合：按 shop_id + spu_id 合并，统计 sku_count、总件数、总申报额
    query = '''
    SELECT 
        p.spu_id,
        p.product_name,
        MAX(p.unit_price) AS unit_price,
        MAX(p.cost_price) AS cost_price,
        COUNT(DISTINCT p.sku_attribute) AS sku_count,
        COALESCE(SUM(sd.quantity), 0) AS total_sold,
        COALESCE(SUM(sd.total_amount), 0) AS total_sales_amount,
        sh.shop_name
    FROM product_prices p
    JOIN shops sh ON p.shop_id = sh.id
    LEFT JOIN shipping_details sd ON p.shop_id = sd.shop_id AND p.spu_id = sd.spu_id
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
    
    query += " GROUP BY p.shop_id, p.spu_id"
    query += " ORDER BY p.update_date DESC, sh.shop_name, p.spu_id"
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

# 更新商品价格（按 SPU/可选规格）：同时同步发货明细中的单价、总金额，以及商品名称和规格
def update_product_price(shop_name, spu_id, sku_attribute, unit_price, cost_price, product_name=None):
    """
    如果 sku_attribute 为空字符串或 None，则将更新该 shop + spu 下的所有 sku_attribute（按 SPU 同步）。
    否则只更新匹配的 sku_attribute（兼容旧行为）。
    """
    shop_id = get_shop_id(shop_name)
    if not shop_id:
        return False
    
    conn = sqlite3.connect('settlement_system.db')
    try:
        cursor = conn.cursor()
        if sku_attribute is None or sku_attribute == '':
            # 更新所有该 SPU 的价格与名称
            cursor.execute('''
            UPDATE product_prices
            SET unit_price = ?, cost_price = ?, product_name = ?, update_date = CURRENT_TIMESTAMP
            WHERE shop_id = ? AND spu_id = ?
            ''', (unit_price, cost_price, product_name if product_name is not None else '', shop_id, spu_id))
            
            cursor.execute('''
            UPDATE shipping_details
            SET unit_price = ?, total_amount = quantity * ?, product_name = ?
            WHERE shop_id = ? AND spu_id = ?
            ''', (unit_price, unit_price, product_name if product_name is not None else '', shop_id, spu_id))
        else:
            # 仅更新特定 sku_attribute
            cursor.execute('''
            UPDATE product_prices
            SET unit_price = ?, cost_price = ?, product_name = ?, update_date = CURRENT_TIMESTAMP
            WHERE shop_id = ? AND spu_id = ? AND sku_attribute = ?
            ''', (unit_price, cost_price, product_name if product_name is not None else '', shop_id, spu_id, sku_attribute))
            
            cursor.execute('''
            UPDATE shipping_details
            SET unit_price = ?, total_amount = quantity * ?, product_name = ?
            WHERE shop_id = ? AND spu_id = ? AND sku_attribute = ?
            ''', (unit_price, unit_price, product_name if product_name is not None else '', shop_id, spu_id, sku_attribute))
        
        conn.commit()
        return True
    except Exception as e:
        print(f"更新商品价格出错: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

# 获取销售分析（新增）
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

# 发货与结款对比分析（新增）
def compare_shipping_settlement(shop_name, start_date=None, end_date=None):
    shop_id = get_shop_id(shop_name)
    if not shop_id:
        return pd.DataFrame()
    
    conn = sqlite3.connect('settlement_system.db')
    
    # 获取发货数据
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
    
    # 如果没有发货数据，返回空
    if len(shipping_df) == 0:
        conn.close()
        return pd.DataFrame()
    
    # 获取结款数据
    stock_order_ids = tuple(shipping_df['stock_order_id'].unique())
    if len(stock_order_ids) == 1:
        stock_order_ids = f"('{stock_order_ids[0]}')"
    
    settlement_query = f'''
    SELECT 
        stock_order_id,
        SUM(CASE WHEN transaction_type = '销售回款' THEN amount ELSE 0 END) as settlement_amount,
        SUM(CASE WHEN transaction_type = '销售回款' THEN quantity ELSE 0 END) as settlement_quantity,
        COUNT(CASE WHEN transaction_type = '销售回款' THEN 1 END) as settlement_count
    FROM transaction_settlements 
    WHERE shop_id = ? AND stock_order_id IN {stock_order_ids}
    GROUP BY stock_order_id
    '''
    
    settlement_params = [shop_id]
    settlement_df = pd.read_sql_query(settlement_query, conn, params=settlement_params)
    
    conn.close()
    
    # 合并发货和结款数据
    if len(settlement_df) > 0:
        result_df = pd.merge(
            shipping_df, 
            settlement_df, 
            on='stock_order_id', 
            how='left'
        )
    else:
        result_df = shipping_df.copy()
        result_df['settlement_amount'] = 0
        result_df['settlement_quantity'] = 0
        result_df['settlement_count'] = 0
    
    # 计算结款率
    result_df['settlement_rate'] = result_df.apply(
        lambda row: (row['settlement_amount'] / row['shipping_amount'] * 100) if row['shipping_amount'] > 0 else 0,
        axis=1
    )
    
    # 重新排列列顺序
    columns = ['stock_order_id', 'spu_id', 'skc_id', 'sku_id', 'product_name', 'sku_attribute',
               'shipping_date', 'shipping_quantity', 'shipping_amount',
               'settlement_quantity', 'settlement_amount', 'settlement_count', 'settlement_rate']
    
    # 只保留存在的列
    existing_columns = [col for col in columns if col in result_df.columns]
    result_df = result_df[existing_columns]
    
    return result_df

# 清除所有数据（用于测试）
def clear_all_data():
    conn = sqlite3.connect('settlement_system.db')
    cursor = conn.cursor()
    
    # 删除所有数据，保留表结构
    cursor.execute("DELETE FROM after_sales")
    cursor.execute("DELETE FROM transaction_settlements")
    cursor.execute("DELETE FROM daily_summary")
    cursor.execute("DELETE FROM shipping_details")
    cursor.execute("DELETE FROM product_prices")
    
    conn.commit()
    conn.close()
    print("✅ 所有数据已清除！")

# 获取所有日期（用于调试）
def get_all_dates():
    conn = sqlite3.connect('settlement_system.db')
    cursor = conn.cursor()
    
    # 从交易表中获取所有日期
    cursor.execute("SELECT DISTINCT settlement_date FROM transaction_settlements WHERE settlement_date IS NOT NULL ORDER BY settlement_date DESC")
    transaction_dates = [row[0] for row in cursor.fetchall()]
    
    # 从售后表中获取所有日期
    cursor.execute("SELECT DISTINCT settlement_date FROM after_sales WHERE settlement_date IS NOT NULL ORDER BY settlement_date DESC")
    after_sales_dates = [row[0] for row in cursor.fetchall()]
    
    # 从发货表中获取所有日期
    cursor.execute("SELECT DISTINCT shipping_date FROM shipping_details WHERE shipping_date IS NOT NULL ORDER BY shipping_date DESC")
    shipping_dates = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    
    return {
        'transaction_dates': transaction_dates,
        'after_sales_dates': after_sales_dates,
        'shipping_dates': shipping_dates
    }

# 检查交易记录是否已存在（基于SKU ID + 账务时间 + 交易类型）
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

# 检查售后记录是否已存在（基于违规ID + SKU ID + 账务时间）
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

# 检查发货明细是否已存在（基于备货单号 + SKU ID）
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

# 调试函数：查看数据
def debug_data():
    conn = sqlite3.connect('settlement_system.db')
    cursor = conn.cursor()
    
    # 统计各表数据量
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
    
    # 查看几个示例备货单号
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

# 主程序入口
if __name__ == "__main__":
    print("=" * 50)
    print("维鲸运营系统 - 数据库模块")
    print("=" * 50)
    
    # 初始化数据库
    init_database()
    print("✓ 数据库初始化完成")
    
    # 显示调试信息
    debug_info = debug_data()
    print("\n数据库状态:")
    print(f"  售后问题记录: {debug_info['after_sales_count']}")
    print(f"  交易结算记录: {debug_info['transaction_count']}")
    print(f"  日汇总记录: {debug_info['summary_count']}")
    print(f"  发货明细记录: {debug_info['shipping_count']}")
    print(f"  商品价格记录: {debug_info['product_count']}")
    
    # 测试日期解析
    print("\n日期解析测试:")
    test_cases = ["WB2510162836467", "WB2512311234567", "WB2501010000001"]
    for test_id in test_cases:
        date = parse_date_from_stock_id(test_id)
        print(f"  {test_id} -> {date}")
    
    print("\n✓ 系统就绪！")
    print("使用方法:")
    print("1. 导入模块: import app")
    print("2. 调用函数: app.insert_transactions(df, '店铺名')")