from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, or_, desc, case, and_, extract
from datetime import datetime, date, timedelta
import pandas as pd
import os
import re
from itertools import groupby
import calendar
import socket

app = Flask(__name__)
app.secret_key = 'weijing_secret_key'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///weijing.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# ==================== 1. 数据库模型 ====================

class UploadRecord(db.Model):
    __tablename__ = 'upload_records'
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(200))
    shop_name = db.Column(db.String(50))
    upload_type = db.Column(db.String(20))
    upload_date = db.Column(db.DateTime, default=datetime.now)
    row_count = db.Column(db.Integer, default=0)

class Product(db.Model):
    __tablename__ = 'products'
    id = db.Column(db.Integer, primary_key=True)
    shop_name = db.Column(db.String(50))
    spu_id = db.Column(db.String(100))
    skc_id = db.Column(db.String(100))
    name = db.Column(db.String(200))
    specs = db.Column(db.String(200))
    declared_price = db.Column(db.Float, default=0.0) 
    cost_price = db.Column(db.Float, default=0.0)     

class Shipment(db.Model):
    __tablename__ = 'shipments'
    id = db.Column(db.Integer, primary_key=True)
    shop_name = db.Column(db.String(50))
    order_no = db.Column(db.String(100), index=True) 
    custom_sku = db.Column(db.String(200), unique=True, index=True)
    date = db.Column(db.Date)
    spu_id = db.Column(db.String(100))
    skc_id = db.Column(db.String(100))
    goods_name = db.Column(db.String(200))
    specs = db.Column(db.String(200))
    quantity = db.Column(db.Integer, default=0)
    declared_price_total = db.Column(db.Float, default=0.0)
    cost_price_total = db.Column(db.Float, default=0.0)
    upload_id = db.Column(db.Integer, db.ForeignKey('upload_records.id'), nullable=True)

class DailyStat(db.Model):
    __tablename__ = 'daily_stats'
    date = db.Column(db.Date, primary_key=True)
    shop_name = db.Column(db.String(50), primary_key=True) 
    total_activity = db.Column(db.Float, default=0.0)
    total_service = db.Column(db.Float, default=0.0)
    total_ad = db.Column(db.Float, default=0.0)
    delivery_fine = db.Column(db.Float, default=0.0)
    total_cost = db.Column(db.Float, nullable=True)

class Settlement(db.Model):
    __tablename__ = 'settlements'
    id = db.Column(db.Integer, primary_key=True)
    shop_name = db.Column(db.String(50))
    order_no = db.Column(db.String(100))
    sku_id = db.Column(db.String(100))
    account_date = db.Column(db.Date)
    amount = db.Column(db.Float, default=0.0)
    sales_income = db.Column(db.Float, default=0.0) 
    sales_refund = db.Column(db.Float, default=0.0) 
    subsidy = db.Column(db.Float, default=0.0)
    platform_fine = db.Column(db.Float, default=0.0)
    trans_type = db.Column(db.String(50))
    violation_id = db.Column(db.String(100))
    upload_id = db.Column(db.Integer, db.ForeignKey('upload_records.id'), nullable=True)

# ==================== 2. 辅助函数 ====================

def extract_date_from_order(order_no):
    try:
        match = re.search(r'WB(\d{6})', str(order_no))
        if match:
            date_str = match.group(1)
            return datetime.strptime(date_str, '%y%m%d').date()
    except: pass
    return date.today()

def find_column(df_columns, possible_names):
    for col in df_columns:
        clean_col = str(col).strip().replace('\t', '').replace('\n', '')
        for p in possible_names:
            if p.lower() in clean_col.lower():
                return col
    return None

def clean_quantity(val):
    try:
        if isinstance(val, (int, float)): return int(val)
        val_str = str(val)
        nums = re.findall(r'\d+', val_str)
        if nums: return int(nums[0])
    except: pass
    return 1

# ==================== 3. 路由与逻辑 ====================

@app.route('/')
def index():
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    latest_shipment = Shipment.query.order_by(Shipment.date.desc()).first()
    default_end = latest_shipment.date if latest_shipment else date.today()
    default_start = default_end - timedelta(days=29)
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else default_start
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date() if end_date_str else default_end
    today = date.today()

    today_activity = db.session.query(func.sum(DailyStat.total_activity)).filter(DailyStat.date == today).scalar() or 0.0
    today_orders = db.session.query(func.sum(Shipment.quantity)).filter(Shipment.date == today).scalar() or 0
    today_settlement = db.session.query(func.sum(Settlement.sales_income)).filter(Settlement.account_date == today).scalar() or 0.0
    
    total_all_activity = db.session.query(func.sum(DailyStat.total_activity)).scalar() or 0.0
    total_all_settled = db.session.query(func.sum(Settlement.sales_income)).scalar() or 0.0
    total_pending = total_all_activity - total_all_settled

    ship_trend = db.session.query(Shipment.date, func.sum(Shipment.quantity).label('qty')).filter(Shipment.date >= start_date, Shipment.date <= end_date).group_by(Shipment.date).all()
    ship_map = {item.date: item.qty for item in ship_trend}
    settle_trend = db.session.query(Settlement.account_date, func.sum(Settlement.sales_income).label('income')).filter(Settlement.account_date >= start_date, Settlement.account_date <= end_date).group_by(Settlement.account_date).all()
    settle_map = {item.account_date: item.income for item in settle_trend}

    all_dates = sorted(list(set(list(ship_map.keys()) + list(settle_map.keys()))))
    chart_dates, chart_shipments, chart_settlements = [], [], []
    for d in all_dates:
        chart_dates.append(d.strftime('%Y-%m-%d'))
        chart_shipments.append(ship_map.get(d, 0))
        chart_settlements.append(settle_map.get(d, 0.0))

    shop_dist = db.session.query(Shipment.shop_name, func.sum(Shipment.quantity).label('qty')).filter(Shipment.date >= start_date, Shipment.date <= end_date).group_by(Shipment.shop_name).order_by(desc('qty')).all()
    shop_labels = [s.shop_name for s in shop_dist]
    shop_data = [s.qty for s in shop_dist]
    alert_list = db.session.query(Settlement).filter(or_(Settlement.platform_fine < 0, Settlement.trans_type.like('%罚款%'))).order_by(Settlement.account_date.desc()).limit(5).all()

    return render_template('index.html', start_date=start_date, end_date=end_date, today_activity=today_activity, today_orders=today_orders, today_settlement=today_settlement, total_pending=total_pending, chart_dates=chart_dates, chart_shipments=chart_shipments, chart_settlements=chart_settlements, shop_labels=shop_labels, shop_data=shop_data, alert_list=alert_list)

@app.route('/shipment')
def shipment():
    page = request.args.get('page', 1, type=int)
    shop_filter = request.args.get('shop_name', '所有店铺')
    today = date.today()
    year = request.args.get('year', type=int, default=today.year)
    month = int(request.args.get('month')) if request.args.get('month') and request.args.get('month')!='0' else None
    day = int(request.args.get('day')) if request.args.get('day') and request.args.get('day')!='0' else None
    
    filters = [extract('year', Shipment.date) == year]
    if month: filters.append(extract('month', Shipment.date) == month)
    if day: filters.append(extract('day', Shipment.date) == day)
    if shop_filter != '所有店铺': filters.append(Shipment.shop_name == shop_filter)

    if not month:
        trend_q = db.session.query(func.strftime('%Y-%m', Shipment.date).label('d'), func.sum(Shipment.quantity)).filter(and_(*filters)).group_by('d')
    else:
        trend_q = db.session.query(Shipment.date, func.sum(Shipment.quantity)).filter(and_(*filters)).group_by(Shipment.date)
    
    trend_data = trend_q.all()
    if not month: trend_dates = [str(d[0]) for d in trend_data]
    else: trend_dates = [d[0].strftime('%m-%d') for d in trend_data]
    trend_values = [d[1] for d in trend_data]

    dist_q = db.session.query(Shipment.shop_name, func.sum(Shipment.quantity).label('qty')).filter(and_(*filters)).group_by(Shipment.shop_name).order_by(desc('qty'))
    s_data = dist_q.all(); shop_labels = [s[0] for s in s_data]; shop_values = [s[1] for s in s_data]

    daily_data = []
    if month is None:
        ship_rows = db.session.query(extract('month', Shipment.date).label('m'), func.sum(Shipment.quantity).label('qty'), func.sum(Shipment.declared_price_total).label('dec'), func.sum(Shipment.cost_price_total).label('cost')).filter(and_(*filters)).group_by('m').all()
        ds_filters = [extract('year', DailyStat.date) == year]
        if shop_filter != '所有店铺': ds_filters.append(DailyStat.shop_name == shop_filter)
        ds_rows = db.session.query(extract('month', DailyStat.date).label('m'), func.sum(DailyStat.total_activity).label('act'), func.sum(DailyStat.total_service).label('srv'), func.sum(DailyStat.total_ad).label('ad'), func.sum(DailyStat.delivery_fine).label('fine'), func.sum(DailyStat.total_cost).label('manual_cost')).filter(and_(*ds_filters)).group_by('m').all()
        st_filters = [extract('year', Settlement.account_date) == year]
        if shop_filter != '所有店铺': st_filters.append(Settlement.shop_name == shop_filter)
        st_rows = db.session.query(extract('month', Settlement.account_date).label('m'), func.sum(Settlement.platform_fine).label('fine'), func.sum(Settlement.sales_refund).label('ref')).filter(and_(*st_filters)).group_by('m').all()

        monthly_map = {m: {'date': f"{year}年{m}月", 'total_quantity':0, 'total_declared':0, 'total_cost':0, 'total_service':0, 'total_activity':0, 'total_fine':0, 'total_refund':0, 'total_ad':0} for m in range(1, 13)}
        for r in ship_rows: d=monthly_map[int(r.m)]; d['total_quantity']=r.qty or 0; d['total_declared']=r.dec or 0; d['total_cost']=r.cost or 0
        for r in ds_rows: d=monthly_map[int(r.m)]; d['total_activity']=r.act or 0; d['total_service']=r.srv or 0; d['total_ad']=r.ad or 0
        for r in st_rows: d=monthly_map[int(r.m)]; d['total_fine']=r.fine or 0; d['total_refund']=abs(r.ref or 0)
        pagination = None
        for m in sorted(monthly_map.keys(), reverse=True):
            if monthly_map[m]['total_quantity'] > 0 or monthly_map[m]['total_activity'] > 0: daily_data.append(monthly_map[m])
    else:
        shipment_query = db.session.query(Shipment.date, func.sum(Shipment.quantity).label('total_quantity'), func.sum(Shipment.declared_price_total).label('total_declared'), func.sum(Shipment.cost_price_total).label('total_cost')).filter(and_(*filters)).group_by(Shipment.date).order_by(Shipment.date.desc())
        pagination = shipment_query.paginate(page=page, per_page=31)
        for item in pagination.items:
            if shop_filter == '所有店铺': daily_stat = db.session.query(func.sum(DailyStat.total_activity).label('total_activity'), func.sum(DailyStat.total_service).label('total_service'), func.sum(DailyStat.total_ad).label('total_ad'), func.sum(DailyStat.delivery_fine).label('delivery_fine'), func.sum(DailyStat.total_cost).label('total_cost')).filter(DailyStat.date == item.date).first()
            else: daily_stat = DailyStat.query.filter_by(date=item.date, shop_name=shop_filter).first()
            settle_q = db.session.query(func.sum(Settlement.platform_fine).label('today_fine'), func.sum(Settlement.sales_refund).label('today_refund')).filter(Settlement.account_date == item.date)
            if shop_filter != '所有店铺': settle_q = settle_q.filter(Settlement.shop_name == shop_filter)
            settlement_stats = settle_q.first()
            calc_cost = item.total_cost or 0.0; manual_cost = (daily_stat.total_cost or 0.0) if daily_stat else 0.0
            daily_data.append({'date': item.date, 'total_quantity': item.total_quantity or 0, 'total_declared': item.total_declared or 0.0, 'total_cost': manual_cost if manual_cost > 0 else calc_cost, 'total_service': (daily_stat.total_service or 0.0) if daily_stat else 0.0, 'total_activity': (daily_stat.total_activity or 0.0) if daily_stat else 0.0, 'total_fine': (settlement_stats.today_fine or 0.0), 'total_refund': abs(settlement_stats.today_refund or 0.0), 'total_ad': (daily_stat.total_ad or 0.0) if daily_stat else 0.0})

    sum_data = {'quantity':0,'declared':0,'cost':0,'service':0,'activity':0,'fine':0,'refund':0,'gross_profit':0,'ad':0}
    for row in daily_data:
        row['gross_profit'] = row['total_activity'] - row['total_cost'] - row['total_service'] - row['total_fine'] - row['total_refund'] - row['total_ad']
        row['roi'] = row['gross_profit'] / row['total_cost'] if row['total_cost'] > 0 else 0.0
        t_q=row['total_quantity']; t_d=row['total_declared']; t_act=row['total_activity']
        row['declared_per_ticket'] = (t_d/t_q) if t_q else 0
        row['profit_per_ticket'] = (row['gross_profit']/t_q) if t_q else 0
        row['refund_rate'] = (row['total_refund']/t_act) if t_act else 0
        row['ad_sales_ratio'] = (row['total_ad']/t_act) if t_act else 0
        row['ad_profit_ratio'] = (row['total_ad']/row['gross_profit']) if row['gross_profit'] else 0
        row['cost_sales_ratio'] = (row['total_cost']/t_act) if t_act else 0
        row['sales_profit_rate'] = (row['gross_profit']/t_act) if t_act else 0
        row['actual_discount_rate'] = (t_act/t_d) if t_d else 0
        sum_data['quantity']+=t_q; sum_data['declared']+=t_d; sum_data['cost']+=row['total_cost']; sum_data['service']+=row['total_service']; sum_data['activity']+=t_act; sum_data['fine']+=row['total_fine']; sum_data['refund']+=row['total_refund']; sum_data['gross_profit']+=row['gross_profit']; sum_data['ad']+=row['total_ad']

    if daily_data:
        s=sum_data
        daily_data.insert(0, {'date':'合计', 'is_summary':True, 'total_quantity':s['quantity'], 'total_declared':s['declared'], 'total_cost':s['cost'], 'total_service':s['service'], 'total_activity':s['activity'], 'total_fine':s['fine'], 'total_refund':s['refund'], 'gross_profit':s['gross_profit'], 'total_ad':s['ad'], 'roi': s['gross_profit']/s['cost'] if s['cost'] else 0, 'declared_per_ticket': (s['declared']/s['quantity']) if s['quantity'] else 0, 'profit_per_ticket': (s['gross_profit']/s['quantity']) if s['quantity'] else 0, 'refund_rate': (s['refund']/s['activity']) if s['activity'] else 0, 'ad_sales_ratio': (s['ad']/s['activity']) if s['activity'] else 0, 'ad_profit_ratio': (s['ad']/s['gross_profit']) if s['gross_profit'] else 0, 'cost_sales_ratio': (s['cost']/s['activity']) if s['activity'] else 0, 'sales_profit_rate': (s['gross_profit']/s['activity']) if s['activity'] else 0, 'actual_discount_rate': (s['activity']/s['declared']) if s['declared'] else 0})

    return render_template('shipment.html', title="发货明细", daily_data=daily_data, pagination=pagination, trend_dates=trend_dates, trend_values=trend_values, shop_labels=shop_labels, shop_values=shop_values, current_shop=shop_filter, selected_year=year, selected_month=month, selected_day=day)

@app.route('/shipment/update_daily', methods=['POST'])
def update_daily_stat():
    try:
        data = request.json; date_str = data.get('date'); shop_name = data.get('shop_name')
        if not shop_name or shop_name == '所有店铺': return jsonify({'status': 'error', 'msg': '请选择具体店铺！'})
        if '年' in str(date_str): return jsonify({'status': 'error', 'msg': '不支持月报模式录入，请切换到具体日期！'})
        target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        stat = DailyStat.query.filter_by(date=target_date, shop_name=shop_name).first()
        if not stat: stat = DailyStat(date=target_date, shop_name=shop_name); db.session.add(stat)
        if 'total_activity' in data: stat.total_activity = float(data.get('total_activity', 0))
        if 'total_service' in data: stat.total_service = float(data.get('total_service', 0))
        if 'total_ad' in data: stat.total_ad = float(data.get('total_ad', 0))
        if 'delivery_fine' in data: stat.delivery_fine = float(data.get('delivery_fine', 0))
        if 'total_cost' in data: stat.total_cost = float(data.get('total_cost', 0))
        db.session.commit()
        return jsonify({'status': 'success', 'msg': '已保存'})
    except Exception as e: return jsonify({'status': 'error', 'msg': str(e)})

@app.route('/shipment/upload', methods=['POST'])
def upload_shipment():
    if 'file' not in request.files: return redirect(url_for('shipment'))
    files = request.files.getlist('file')
    shop_name_selected = request.form.get('shop_name')
    if not files or files[0].filename == '': return redirect(url_for('shipment'))
    try:
        for file in files:
            if file.filename == '': continue
            upload_rec = UploadRecord(filename=file.filename, shop_name=shop_name_selected, upload_type='shipment', row_count=0)
            db.session.add(upload_rec); db.session.flush()

            df = pd.read_csv(file) if file.filename.endswith('.csv') else pd.read_excel(file)
            col_order = find_column(df.columns, ['备货单', '发货单', '订单号', 'order_no'])
            col_custom_sku = find_column(df.columns, ['定制SKU', 'custom_sku', 'Custom SKU']) 
            col_spu = find_column(df.columns, ['商品SPU ID', 'SPUID', 'spu_id'])
            col_skc = find_column(df.columns, ['商品SKC ID', 'SKCID', 'skc_id'])
            col_name = find_column(df.columns, ['商品名称', 'Title', 'name'])
            col_specs = find_column(df.columns, ['商品属性集', '规格', 'SKU属性'])
            col_qty = find_column(df.columns, ['总发货件数', '数量', '件数', 'Quantity'])
            col_shop = find_column(df.columns, ['店铺', '店铺名称'])
            if not col_order: continue
            df[col_order] = df[col_order].ffill()
            if col_shop: df[col_shop] = df[col_shop].ffill()
            count = 0
            for index, row in df.iterrows():
                raw_order_str = str(row[col_order]).strip()
                if '/' in raw_order_str: raw_order_str = raw_order_str.split('/')[-1].strip()
                qty_val = 1; order_no = raw_order_str
                qty_match = re.search(r'[，,]\s*(\d+)\s*件', raw_order_str)
                if qty_match:
                    try: qty_val = int(qty_match.group(1)); order_no = re.split(r'[，,]', raw_order_str)[0].strip()
                    except: qty_val = 1
                if col_qty and pd.notna(row[col_qty]): qty_val = clean_quantity(row[col_qty])
                custom_sku_val = None
                if col_custom_sku and pd.notna(row[col_custom_sku]): raw_sku = str(row[col_custom_sku]).strip(); custom_sku_val = raw_sku if raw_sku and raw_sku!='-' and raw_sku.lower()!='nan' else None
                goods_name = str(row[col_name]).strip() if col_name and pd.notna(row[col_name]) else ''
                if not custom_sku_val: 
                    if not goods_name or goods_name == '-' or goods_name.lower() == 'nan': continue 
                if custom_sku_val:
                    existing = Shipment.query.filter_by(custom_sku=custom_sku_val).first()
                    if existing: continue
                ship_date = extract_date_from_order(order_no)
                current_shop = row[col_shop] if col_shop and pd.notna(row[col_shop]) else shop_name_selected
                if not current_shop or str(current_shop).lower() == 'nan': current_shop = "未知店铺"
                spu_val = str(row[col_spu]) if col_spu and pd.notna(row[col_spu]) else ''
                skc_val = str(row[col_skc]) if col_skc and pd.notna(row[col_skc]) else ''
                specs_val = str(row[col_specs]) if col_specs and pd.notna(row[col_specs]) else ''
                product_record = Product.query.filter_by(shop_name=current_shop, spu_id=spu_val, skc_id=skc_val, specs=specs_val).first()
                unit_declared_price, unit_cost_price = 0.0, 0.0
                if not product_record:
                    new_product = Product(shop_name=current_shop, spu_id=spu_val, skc_id=skc_val, name=goods_name, specs=specs_val)
                    db.session.add(new_product); db.session.flush() 
                else:
                    unit_declared_price = product_record.declared_price; unit_cost_price = product_record.cost_price
                new_shipment = Shipment(shop_name=current_shop, order_no=order_no, custom_sku=custom_sku_val, date=ship_date, spu_id=spu_val, skc_id=skc_val, goods_name=goods_name, specs=specs_val, quantity=qty_val, declared_price_total=unit_declared_price * qty_val, cost_price_total=unit_cost_price * qty_val, upload_id=upload_rec.id)
                db.session.add(new_shipment)
                count += 1
            upload_rec.row_count = count
        db.session.commit()
    except Exception: db.session.rollback()
    return redirect(url_for('shipment'))

@app.route('/settlement')
def settlement():
    page = request.args.get('page', 1, type=int)
    shop_filter = request.args.get('shop_name', '所有店铺')
    today = date.today()
    year = request.args.get('year', type=int, default=today.year)
    month = int(request.args.get('month')) if request.args.get('month') and request.args.get('month')!='0' else None
    day = int(request.args.get('day')) if request.args.get('day') and request.args.get('day')!='0' else None

    filters = [extract('year', Settlement.account_date) == year]
    if month: filters.append(extract('month', Settlement.account_date) == month)
    if day: filters.append(extract('day', Settlement.account_date) == day)
    if shop_filter != '所有店铺': filters.append(Settlement.shop_name == shop_filter)

    trend_q = db.session.query(func.strftime('%Y-%m' if not month else '%Y-%m-%d', Settlement.account_date).label('d'), func.sum(Settlement.sales_income)).filter(and_(*filters)).group_by('d')
    t_data = trend_q.all(); trend_dates=[d[0] for d in t_data]; trend_income=[d[1] for d in t_data]

    total_inc = sum(trend_income)
    ref_q = db.session.query(func.sum(Settlement.sales_refund)).filter(and_(*filters))
    fine_q = db.session.query(func.sum(Settlement.platform_fine)).filter(and_(*filters))
    if shop_filter != '所有店铺': ref_q=ref_q.filter(Settlement.shop_name == shop_filter); fine_q=fine_q.filter(Settlement.shop_name == shop_filter)
    total_refund = ref_q.scalar() or 0.0; total_fine = fine_q.scalar() or 0.0
    
    settlement_query = db.session.query(
        Settlement.account_date,
        func.sum(Settlement.sales_income).label('total_income'),
        func.sum(Settlement.sales_refund).label('total_refund'),
        func.sum(Settlement.subsidy).label('total_subsidy'),
        func.sum(Settlement.platform_fine).label('total_fine'),
        func.count(Settlement.id).label('settlement_count')
    ).filter(and_(*filters)).group_by(Settlement.account_date).order_by(Settlement.account_date.desc())
    
    pagination = settlement_query.paginate(page=page, per_page=31)
    
    report_data = []
    sum_data = {'ship_qty':0,'ship_declared':0,'activity_price':0,'ship_cost':0,'service_fee':0,'ad_cost':0,'settle_count':0,'settle_amount':0,'consumer_refund':0,'subsidy':0,'after_sales_fine':0,'delivery_fine':0,'gross_profit':0}

    for item in pagination.items:
        date_obj = item.account_date
        if shop_filter == '所有店铺':
            daily_stat = db.session.query(func.sum(DailyStat.total_activity).label('total_activity'), func.sum(DailyStat.total_service).label('total_service'), func.sum(DailyStat.total_ad).label('total_ad'), func.sum(DailyStat.delivery_fine).label('delivery_fine'), func.sum(DailyStat.total_cost).label('total_cost')).filter(DailyStat.date == date_obj).first()
        else:
            daily_stat = DailyStat.query.filter_by(date=date_obj, shop_name=shop_filter).first()

        ship_q = db.session.query(func.sum(Shipment.quantity).label('qty'), func.sum(Shipment.declared_price_total).label('declared'), func.sum(Shipment.cost_price_total).label('cost')).filter(Shipment.date == date_obj)
        if shop_filter != '所有店铺': ship_q = ship_q.filter(Shipment.shop_name == shop_filter)
        ship_stat = ship_q.first()
        
        s_inc=item.total_income or 0; s_ref=abs(item.total_refund or 0); s_sub=item.total_subsidy or 0; s_fine=abs(item.total_fine or 0); s_cnt=item.settlement_count or 0
        s_qty=ship_stat.qty or 0; s_dec=ship_stat.declared or 0; s_cost=ship_stat.cost or 0
        
        calc_cost = ship_stat.cost or 0.0
        manual_cost = (daily_stat.total_cost or 0.0) if daily_stat else 0.0
        ship_cost = manual_cost if manual_cost > 0 else calc_cost

        d_ad=(daily_stat.total_ad or 0.0) if daily_stat else 0; d_srv=(daily_stat.total_service or 0.0) if daily_stat else 0; d_act=(daily_stat.total_activity or 0.0) if daily_stat else 0; d_dfine=(daily_stat.delivery_fine or 0.0) if daily_stat else 0
        
        gp = s_inc - d_ad - ship_cost - d_srv - s_ref - s_sub - s_fine - d_dfine
        
        sum_data['ship_qty']+=s_qty; sum_data['ship_declared']+=s_dec; sum_data['activity_price']+=d_act; sum_data['ship_cost']+=ship_cost; sum_data['service_fee']+=d_srv; sum_data['ad_cost']+=d_ad
        sum_data['settle_count']+=s_cnt; sum_data['settle_amount']+=s_inc; sum_data['consumer_refund']+=s_ref; sum_data['subsidy']+=s_sub; sum_data['after_sales_fine']+=s_fine; sum_data['delivery_fine']+=d_dfine; sum_data['gross_profit']+=gp
        
        report_data.append({
            'date':date_obj, 'ship_qty':s_qty, 'ship_declared':s_dec, 'activity_price':d_act, 'activity_discount':d_act/s_dec if s_dec else 0,
            'ship_cost':ship_cost, 'service_fee':d_srv, 'ad_cost':d_ad, 'settle_count':s_cnt, 'settle_count_ratio':s_cnt/s_qty if s_qty else 0,
            'settle_amount':s_inc, 'settle_amount_discount':s_inc/s_dec if s_dec else 0, 'settle_amount_progress':s_inc/d_act if d_act else 0,
            'consumer_refund':s_ref, 'subsidy':s_sub, 'after_sales_fine':s_fine, 'delivery_fine':d_dfine, 'gross_profit':gp, 'roi':gp/(ship_cost+d_srv) if (ship_cost+d_srv) else 0,
            'declared_per_ticket':s_dec/s_qty if s_qty else 0, 'profit_per_ticket':gp/s_qty if s_qty else 0, 'discount_diff': (s_inc/s_dec - d_act/s_dec) if s_dec else 0,
            'refund_rate':s_ref/s_inc if s_inc else 0, 'after_sales_rate':s_fine/s_inc if s_inc else 0, 'delivery_fine_rate':d_dfine/s_inc if s_inc else 0
        })

    if report_data:
        sd=sum_data
        summary={'date':'合计', 'is_summary':True, 'ship_qty':sd['ship_qty'], 'ship_declared':sd['ship_declared'], 'activity_price':sd['activity_price'], 'activity_discount':sd['activity_price']/sd['ship_declared'] if sd['ship_declared'] else 0, 'ship_cost':sd['ship_cost'], 'service_fee':sd['service_fee'], 'ad_cost':sd['ad_cost'], 'settle_count':sd['settle_count'], 'settle_count_ratio':sd['settle_count']/sd['ship_qty'] if sd['ship_qty'] else 0, 'settle_amount':sd['settle_amount'], 'settle_amount_discount':sd['settle_amount']/sd['ship_declared'] if sd['ship_declared'] else 0, 'settle_amount_progress':sd['settle_amount']/sd['activity_price'] if sd['activity_price'] else 0, 'consumer_refund':sd['consumer_refund'], 'subsidy':sd['subsidy'], 'after_sales_fine':sd['after_sales_fine'], 'delivery_fine':sd['delivery_fine'], 'gross_profit':sd['gross_profit'], 'roi':sd['gross_profit']/(sd['ship_cost']+sd['service_fee']) if (sd['ship_cost']+sd['service_fee']) else 0, 'declared_per_ticket':sd['ship_declared']/sd['ship_qty'] if sd['ship_qty'] else 0, 'profit_per_ticket':sd['gross_profit']/sd['ship_qty'] if sd['ship_qty'] else 0, 'discount_diff':0, 'refund_rate':sd['consumer_refund']/sd['settle_amount'] if sd['settle_amount'] else 0, 'after_sales_rate':sd['after_sales_fine']/sd['settle_amount'] if sd['settle_amount'] else 0, 'delivery_fine_rate':sd['delivery_fine']/sd['settle_amount'] if sd['settle_amount'] else 0}
        report_data.insert(0, summary)
    
    return render_template('settlement.html', title="结算明细", report_data=report_data, pagination=pagination, trend_dates=trend_dates, trend_income=trend_income, pie_data=[total_inc, abs(total_refund), abs(total_fine)], current_shop=shop_filter, selected_year=year, selected_month=month, selected_day=day)

# [修改] 上传结算 - 增加错误处理和CSV修正
@app.route('/settlement/upload', methods=['POST'])
def upload_settlement():
    if 'file' not in request.files: return redirect(url_for('settlement'))
    files = request.files.getlist('file')
    shop_name = request.form.get('shop_name')
    if not files or files[0].filename == '': return redirect(url_for('settlement'))
    
    try:
        def process_trans(df):
            count = 0
            col_order = find_column(df.columns, ['备货单号', '订单号'])
            col_sku = find_column(df.columns, ['SKUID', 'SKU ID'])
            col_type = find_column(df.columns, ['交易类型'])
            col_amount = find_column(df.columns, ['金额', '发生金额'])
            for index, row in df.iterrows():
                if not col_order: continue
                order_no = str(row[col_order]).strip()
                trans_type = str(row[col_type]).strip() if col_type else ''
                amount = float(row[col_amount]) if col_amount and pd.notna(row[col_amount]) else 0.0
                sku_val = str(row[col_sku]) if col_sku and pd.notna(row[col_sku]) else ''
                acc_date = extract_date_from_order(order_no)
                exists = Settlement.query.filter_by(sku_id=sku_val, account_date=acc_date, trans_type=trans_type, order_no=order_no, amount=amount).first()
                if exists: continue
                s_income=0; s_refund=0; s_subsidy=0
                if '售后' in trans_type or '冲回' in trans_type: s_refund = amount
                elif '补贴' in trans_type: s_subsidy = amount
                else: s_income = amount
                new_rec = Settlement(shop_name=shop_name, order_no=order_no, sku_id=sku_val, account_date=acc_date, trans_type=trans_type, amount=amount, sales_income=s_income, sales_refund=s_refund, subsidy=s_subsidy, platform_fine=0, upload_id=upload_rec.id)
                db.session.add(new_rec)
                count += 1
            return count

        def process_fine(df):
            count = 0
            col_vid = find_column(df.columns, ['违规ID', '违规编号'])
            col_sku_f = find_column(df.columns, ['SKUID'])
            col_amt_f = find_column(df.columns, ['赔付金额', '扣款金额'])
            col_date_f = find_column(df.columns, ['账务时间'])
            for index, row in df.iterrows():
                if not col_vid: continue
                vid = str(row[col_vid]).strip()
                if Settlement.query.filter_by(violation_id=vid).first(): continue
                acc_date = pd.to_datetime(row[col_date_f]).date()
                amt = float(row[col_amt_f]) if pd.notna(row[col_amt_f]) else 0.0
                sku_val = str(row[col_sku_f]) if col_sku_f else ''
                new_rec = Settlement(shop_name=shop_name, violation_id=vid, sku_id=sku_val, account_date=acc_date, trans_type='售后罚款', amount=amt, platform_fine=amt, upload_id=upload_rec.id)
                db.session.add(new_rec)
                count += 1
            return count

        for file in files:
            if file.filename == '': continue
            upload_rec = UploadRecord(filename=file.filename, shop_name=shop_name, upload_type='settlement', row_count=0)
            db.session.add(upload_rec); db.session.flush()
            
            # [关键修复]：文件指针复位 + 多编码尝试
            try:
                if file.filename.endswith('.csv'):
                    file.seek(0)
                    try: df = pd.read_csv(file)
                    except: file.seek(0); df = pd.read_csv(file, encoding='gbk')
                    if find_column(df.columns, ['交易类型']): upload_rec.row_count = process_trans(df)
                    elif find_column(df.columns, ['违规ID', '违规编号']): upload_rec.row_count = process_fine(df)
                else:
                    excel_file = pd.ExcelFile(file)
                    if '交易结算' in excel_file.sheet_names: upload_rec.row_count += process_trans(pd.read_excel(file, sheet_name='交易结算'))
                    if '消费者及履约保障-售后问题' in excel_file.sheet_names: upload_rec.row_count += process_fine(pd.read_excel(file, sheet_name='消费者及履约保障-售后问题'))
            except Exception as e:
                print(f"Error parsing file {file.filename}: {e}")
                flash(f"解析错误: {file.filename}", 'error')
        
        db.session.commit()
    except Exception as e: db.session.rollback(); flash(f"上传失败: {str(e)}", 'error')
    return redirect(url_for('settlement'))

# ... (Files, Product, search, clear_data, main 等代码与之前一致，请务必保留)
@app.route('/files')
def files():
    records = UploadRecord.query.order_by(UploadRecord.upload_date.desc()).all()
    return render_template('files.html', title="文件管理", records=records)

@app.route('/files/delete/<int:record_id>', methods=['POST'])
def delete_file(record_id):
    record = UploadRecord.query.get_or_404(record_id)
    try:
        if record.upload_type == 'shipment': Shipment.query.filter_by(upload_id=record.id).delete()
        elif record.upload_type == 'settlement': Settlement.query.filter_by(upload_id=record.id).delete()
        db.session.delete(record); db.session.commit()
        return jsonify({'status': 'success', 'msg': '删除成功'})
    except Exception as e: db.session.rollback(); return jsonify({'status': 'error', 'msg': str(e)})

@app.route('/product')
def product():
    page = request.args.get('page', 1, type=int)
    shop_filter = request.args.get('shop_name', '所有店铺')
    search_keyword = request.args.get('q', '').strip() 
    filter_missing = request.args.get('filter_missing', 'false') == 'true'

    query = Product.query
    if shop_filter != '所有店铺':
        query = query.filter_by(shop_name=shop_filter)
    
    missing_query = query.filter(or_(Product.declared_price == 0, Product.cost_price == 0))
    missing_count = missing_query.count()

    if search_keyword:
        filters = [
            Product.spu_id.contains(search_keyword),
            Product.skc_id.contains(search_keyword),
            Product.name.contains(search_keyword)
        ]
        if search_keyword.isdigit():
             filters.append(Product.spu_id.contains(f"{search_keyword}.0"))
             filters.append(Product.skc_id.contains(f"{search_keyword}.0"))
        query = query.filter(or_(*filters))

    if filter_missing:
        query = query.filter(or_(Product.declared_price == 0, Product.cost_price == 0))

    all_products = query.order_by(Product.spu_id.desc(), Product.id.desc()).all()
    
    grouped_products = []
    for spu_id, items in groupby(all_products, key=lambda x: x.spu_id):
        items_list = list(items)
        if not items_list: continue
        first_item = items_list[0]
        group_data = {'shop_name': first_item.shop_name, 'spu_id': spu_id, 'skc_id': first_item.skc_id, 'name': first_item.name, 'variants': items_list, 'count': len(items_list)}
        grouped_products.append(group_data)
        
    per_page = 10
    start = (page - 1) * per_page
    current_page_data = grouped_products[start:start+per_page]
    
    class Pagination:
        def __init__(self, page, per_page, total):
            self.page, self.per_page, self.total, self.pages = page, per_page, total, (total + per_page - 1) // per_page
            self.has_prev, self.has_next, self.prev_num, self.next_num = page > 1, page < self.pages, page - 1, page + 1
    pagination = Pagination(page, per_page, len(grouped_products))
    
    return render_template('product.html', title="商品列表", groups=current_page_data, pagination=pagination, current_shop=shop_filter, search_keyword=search_keyword, missing_count=missing_count, filter_missing=filter_missing)

@app.route('/product/update', methods=['POST'])
def update_product_price():
    try:
        data = request.json
        product = Product.query.get(data.get('id'))
        if product:
            val = float(data.get('value'))
            if data.get('field') == 'declared_price': product.declared_price = val
            elif data.get('field') == 'cost_price': product.cost_price = val
            related = Shipment.query.filter_by(shop_name=product.shop_name, spu_id=product.spu_id, skc_id=product.skc_id, specs=product.specs).all()
            for s in related:
                if data.get('field') == 'declared_price': s.declared_price_total = s.quantity * val
                elif data.get('field') == 'cost_price': s.cost_price_total = s.quantity * val
            db.session.commit()
            return jsonify({'status': 'success'})
        return jsonify({'status': 'error'})
    except Exception as e: return jsonify({'status': 'error', 'msg': str(e)})

@app.route('/search')
def search():
    keyword = request.args.get('q', '').strip()
    shipments, settlements = [], []
    if keyword:
        shipments = Shipment.query.filter(or_(Shipment.order_no.contains(keyword), Shipment.custom_sku.contains(keyword), Shipment.skc_id.contains(keyword), Shipment.spu_id.contains(keyword))).order_by(Shipment.date.desc()).all()
        settlements = Settlement.query.filter(or_(Settlement.order_no.contains(keyword), Settlement.sku_id.contains(keyword))).order_by(Settlement.account_date.desc()).all()
    return render_template('search.html', title="订单搜索", keyword=keyword, shipments=shipments, settlements=settlements)

@app.route('/test/clear', methods=['POST'])
def clear_data():
    try:
        data = request.get_json()
        if not data or data.get('password') != 'caomei521':
             return jsonify({'status': 'error', 'msg': '密码错误，操作已拒绝！'})

        db.session.query(Shipment).delete()
        db.session.query(Settlement).delete()
        db.session.query(Product).delete()
        db.session.query(DailyStat).delete()
        db.session.query(UploadRecord).delete() # 清空记录
        db.session.commit()
        return jsonify({'status': 'success', 'msg': '已清空'})
    except Exception as e: return jsonify({'status': 'error', 'msg': str(e)})

if __name__ == '__main__':
    if not os.path.exists('weijing.db'):
        with app.app_context(): db.create_all()
    
    import socket
    try:
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        print(f" 服务已启动！请在浏览器访问：")
        print(f" 👉 本机访问：http://127.0.0.1:5001")
        print(f" 👉 同事访问：http://{local_ip}:5001")
    except: pass

    app.run(host='0.0.0.0', debug=True, port=5001)