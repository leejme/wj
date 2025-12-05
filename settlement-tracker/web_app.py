#!/usr/bin/env python3
from flask import Flask, render_template, request, jsonify, redirect, url_for
from datetime import datetime
import os
import pandas as pd

# 导入数据库模块
try:
    import app as database
    print("✅ 数据库模块导入成功")
except:
    import database
    print("✅ 使用database模块")

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-here'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max upload

# 首页 - 使用简约模板
@app.route('/')
def index():
    """首页"""
    # 获取今天的数据汇总（这里先用假数据，后面连接数据库）
    today = datetime.now().strftime("%Y-%m-%d")
    
    return render_template('minimal_index.html', 
                          shops=database.SHOP_LIST,
                          today=today)

# 数据看板页面 - 使用简约模板
@app.route('/dashboard')
def dashboard():
    """数据看板"""
    today = datetime.now().strftime("%Y-%m-%d")
    date = request.args.get('date', today)
    
    # 获取所有店铺的汇总数据
    try:
        all_shops_data = database.get_all_shops_summary(date)
    except:
        # 如果数据库没有数据，使用示例数据
        all_shops_data = {
            'shops': [],
            'summary': {
                'total_sales': 0,
                'total_refunds': 0,
                'total_subsidies': 0,
                'total_after_sales': 0
            }
        }
    
    # 获取所有日期用于选择器
    try:
        date_data = database.get_all_dates()
        all_dates = date_data.get('transaction_dates', [])
    except:
        all_dates = []
    
    return render_template('minimal_dashboard.html', 
                          shops=database.SHOP_LIST,
                          today=date,
                          all_shops_data=all_shops_data,
                          all_dates=all_dates)

# 导入数据页面 - 使用简约模板
@app.route('/import', methods=['GET', 'POST'])
def import_data():
    """导入数据页面"""
    if request.method == 'POST':
        # 处理文件上传
        if 'file' not in request.files:
            return jsonify({'error': '没有选择文件'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': '没有选择文件'}), 400
        
        # 检查文件扩展名
        filename = file.filename.lower()
        allowed_extensions = ['.csv', '.xlsx', '.xls']
        file_ext = os.path.splitext(filename)[1]
        
        if file_ext not in allowed_extensions:
            return jsonify({'error': f'不支持的文件格式，请上传CSV或Excel文件'}), 400
        
        shop_name = request.form.get('shop_name')
        data_type = request.form.get('data_type')
        
        if not shop_name or not data_type:
            return jsonify({'error': '请选择店铺和数据类型'}), 400
        
        try:
            # 根据文件类型读取数据
            if file_ext == '.csv':
                df = pd.read_csv(file)
            else:
                # Excel文件
                df = pd.read_excel(file)
            
            # 根据数据类型导入
            inserted = 0
            skipped = 0
            
            if data_type == 'transactions':
                inserted, skipped = database.insert_transactions(df, shop_name)
                database.update_daily_summary(shop_name)
                database.update_all_shops_summary()
            elif data_type == 'after_sales':
                inserted, skipped = database.insert_after_sales(df, shop_name)
                database.update_daily_summary(shop_name)
                database.update_all_shops_summary()
            elif data_type == 'shipping':
                inserted, skipped = database.insert_shipping_details(df, shop_name)
            
            return jsonify({
                'success': True,
                'message': f'导入成功！新增 {inserted} 条记录，跳过 {skipped} 条重复记录',
                'inserted': inserted,
                'skipped': skipped
            })
            
        except Exception as e:
            return jsonify({'error': f'导入失败: {str(e)}'}), 500
    
    return render_template('minimal_import.html', shops=database.SHOP_LIST)

# 发货明细页面 - 使用简约模板
@app.route('/shipping_details')
def shipping_details():
    """发货明细页面"""
    shop_name = request.args.get('shop')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    product_name = request.args.get('product_name')
    spu_id = request.args.get('spu_id')
    stock_order_id = request.args.get('stock_order_id')
    
    results_df = None
    result_count = 0
    
    try:
        results_df = database.search_shipping_details(
            shop_name=shop_name,
            spu_id=spu_id,
            sku_id=None,  # 专门用于发货明细，不按SKU搜索
            stock_order_id=stock_order_id,
            start_date=start_date,
            end_date=end_date
        )
        
        # 如果需要按商品名称筛选
        if product_name and results_df is not None and not results_df.empty:
            results_df = results_df[results_df['product_name'].astype(str).str.contains(product_name, na=False)]
        
        if results_df is not None and not results_df.empty:
            # 转换为字典列表以便在模板中显示
            results = results_df.to_dict('records')
            result_count = len(results)
        else:
            results = []
            result_count = 0
    
    except Exception as e:
        print(f"查询发货明细出错: {e}")
        import traceback
        traceback.print_exc()
        results = []
        result_count = 0
    
    return render_template('minimal_shipping_details.html', 
                          shops=database.SHOP_LIST,
                          results=results,
                          result_count=result_count,
                          current_shop=shop_name,
                          start_date=start_date,
                          end_date=end_date,
                          product_name=product_name,
                          spu_id=spu_id,
                          stock_order_id=stock_order_id)

# 搜索查询页面 - 使用简约模板
@app.route('/search')
def search():
    """搜索查询页面"""
    search_type = request.args.get('type', 'orders')
    shop_name = request.args.get('shop')
    keyword = request.args.get('keyword')
    date = request.args.get('date')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    product_name = request.args.get('product_name')
    spu_id = request.args.get('spu_id')
    
    results_df = None
    result_count = 0
    
    try:
        if search_type == 'orders':
            order_id = keyword if keyword and ('ORD' in str(keyword).upper() or len(keyword) > 10) else None
            stock_order_id = keyword if keyword and 'WB' in str(keyword).upper() else None
            sku_id = keyword if keyword and ('SKU' in str(keyword).upper()) else None
            
            results_df = database.search_orders(
                shop_name=shop_name,
                stock_order_id=stock_order_id,
                order_id=order_id,
                date=date
            )
            
        elif search_type == 'after_sales':
            violation_id = keyword if keyword and ('VIOL' in str(keyword).upper() or '违规' in str(keyword)) else None
            
            results_df = database.search_after_sales(
                shop_name=shop_name,
                violation_id=violation_id,
                date=date
            )
            
        elif search_type == 'shipping':
            stock_order_id = keyword if keyword and 'WB' in str(keyword).upper() else None
            spu_id_search = keyword if keyword and 'SPU' in str(keyword).upper() else None
            sku_id_search = keyword if keyword and 'SKU' in str(keyword).upper() else None
            
            results_df = database.search_shipping_details(
                shop_name=shop_name,
                spu_id=spu_id_search,
                sku_id=sku_id_search,
                stock_order_id=stock_order_id,
                start_date=start_date,
                end_date=end_date
            )
            
        elif search_type == 'products':
            results_df = database.get_products(
                shop_name=shop_name,
                spu_id=spu_id or keyword if keyword and 'SPU' in str(keyword).upper() else None,
                product_name=product_name or keyword if keyword and not 'SPU' in str(keyword).upper() else None
            )
        
        if results_df is not None and not results_df.empty:
            # 转换为字典列表以便在模板中显示
            results = results_df.to_dict('records')
            result_count = len(results)
        else:
            results = []
            result_count = 0
    
    except Exception as e:
        print(f"搜索出错: {e}")
        import traceback
        traceback.print_exc()
        results = []
        result_count = 0
    
    return render_template('minimal_search.html', 
                          shops=database.SHOP_LIST,
                          search_type=search_type,
                          results=results,
                          result_count=result_count,
                          current_shop=shop_name,
                          current_keyword=keyword,
                          current_date=date,
                          start_date=start_date,
                          end_date=end_date,
                          product_name=product_name,
                          spu_id=spu_id)

# 添加导出路由
@app.route('/export/<search_type>')
def export_results(search_type):
    """导出搜索结果"""
    import io
    from flask import send_file
    
    shop_name = request.args.get('shop')
    keyword = request.args.get('keyword')
    date = request.args.get('date')
    
    try:
        if search_type == 'orders':
            results_df = database.search_orders(
                shop_name=shop_name,
                stock_order_id=keyword if keyword and 'WB' in str(keyword).upper() else None,
                order_id=keyword if keyword and 'ORD' in str(keyword).upper() else None,
                date=date
            )
            filename = f"订单搜索_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
        elif search_type == 'after_sales':
            results_df = database.search_after_sales(
                shop_name=shop_name,
                violation_id=keyword,
                date=date
            )
            filename = f"售后搜索_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
        elif search_type == 'shipping':
            results_df = database.search_shipping_details(
                shop_name=shop_name,
                stock_order_id=keyword if keyword and 'WB' in str(keyword).upper() else None,
                spu_id=keyword if keyword and 'SPU' in str(keyword).upper() else None,
                sku_id=keyword if keyword and 'SKU' in str(keyword).upper() else None,
                start_date=request.args.get('start_date'),
                end_date=request.args.get('end_date')
            )
            filename = f"发货搜索_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
        elif search_type == 'products':
            results_df = database.get_products(
                shop_name=shop_name,
                spu_id=request.args.get('spu_id'),
                product_name=request.args.get('product_name')
            )
            filename = f"商品搜索_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        else:
            return jsonify({'error': '不支持的类型'}), 400
        
        if results_df.empty:
            return jsonify({'error': '没有数据可导出'}), 400
        
        # 创建CSV
        output = io.StringIO()
        results_df.to_csv(output, index=False, encoding='utf-8-sig')
        output.seek(0)
        
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8-sig')),
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 商品管理页面
@app.route('/products')
def products():
    """商品管理页面"""
    shop_name = request.args.get('shop')
    spu_id = request.args.get('spu_id')
    product_name = request.args.get('product_name')
    
    try:
        products_df = database.get_products(
            shop_name=shop_name,
            spu_id=spu_id,
            product_name=product_name
        )
        products_data = products_df.to_dict('records') if not products_df.empty else []
    except Exception as e:
        print(f"获取商品列表出错: {e}")
        products_data = []
    
    return render_template('minimal_products.html', 
                          shops=database.SHOP_LIST,
                          products=products_data,
                          current_shop=shop_name,
                          current_spu_id=spu_id,
                          current_product_name=product_name)

# 更新商品价格API
@app.route('/api/update_product_price', methods=['POST'])
def update_product_price():
    """更新商品价格API"""
    try:
        data = request.json
        shop_name = data.get('shop_name')
        spu_id = data.get('spu_id')
        sku_attribute = data.get('sku_attribute')
        unit_price = float(data.get('unit_price', 0))
        cost_price = float(data.get('cost_price', 0))
        
        success = database.update_product_price(
            shop_name, spu_id, sku_attribute, unit_price, cost_price
        )
        
        if success:
            return jsonify({'success': True, 'message': '价格更新成功'})
        else:
            return jsonify({'error': '更新失败'}), 500
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 月度汇总页面
@app.route('/monthly')
def monthly_summary():
    """月度汇总页面"""
    shop_name = request.args.get('shop', '云企')
    year = request.args.get('year', datetime.now().year)
    month = request.args.get('month', datetime.now().month)
    
    try:
        summary = database.get_monthly_summary(shop_name, int(year), int(month))
    except Exception as e:
        print(f"获取月度汇总出错: {e}")
        summary = None
    
    return render_template('minimal_monthly.html',
                          shops=database.SHOP_LIST,
                          summary=summary,
                          current_shop=shop_name,
                          current_year=year,
                          current_month=month)

# 发货与结款对比页面
@app.route('/comparison')
def comparison():
    """发货与结款对比页面"""
    shop_name = request.args.get('shop', '云企')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    try:
        results_df = database.compare_shipping_settlement(
            shop_name, start_date, end_date
        )
        results = results_df.to_dict('records') if not results_df.empty else []
    except Exception as e:
        print(f"获取对比数据出错: {e}")
        results = []
    
    return render_template('minimal_comparison.html',
                          shops=database.SHOP_LIST,
                          results=results,
                          current_shop=shop_name,
                          start_date=start_date,
                          end_date=end_date)

# 数据管理页面
@app.route('/data_management')
def data_management():
    """数据管理页面"""
    try:
        debug_info = database.debug_data()
        
        # 获取所有日期
        date_data = database.get_all_dates()
        transaction_dates = date_data.get('transaction_dates', [])
        after_sales_dates = date_data.get('after_sales_dates', [])
        shipping_dates = date_data.get('shipping_dates', [])
        
        # 统计每个店铺的数据量
        shop_stats = []
        for shop in database.SHOP_LIST:
            if shop != "汇总":
                shop_id = database.get_shop_id(shop)
                if shop_id:
                    # 这里可以添加获取每个店铺数据量的逻辑
                    shop_stats.append({
                        'name': shop,
                        'transactions': 0,
                        'after_sales': 0,
                        'shipping': 0
                    })
    
    except Exception as e:
        print(f"获取数据管理信息出错: {e}")
        debug_info = {}
        transaction_dates = []
        after_sales_dates = []
        shipping_dates = []
        shop_stats = []
    
    return render_template('minimal_data_management.html',
                          shops=database.SHOP_LIST,
                          debug_info=debug_info,
                          transaction_dates=transaction_dates,
                          after_sales_dates=after_sales_dates,
                          shipping_dates=shipping_dates,
                          shop_stats=shop_stats)

# 清除数据API
@app.route('/api/clear_data', methods=['POST'])
def clear_data():
    """清除数据API"""
    try:
        database.clear_all_data()
        return jsonify({'success': True, 'message': '数据已清除'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 获取店铺汇总数据API
@app.route('/api/daily_summary')
def api_daily_summary():
    """获取日汇总数据API"""
    date = request.args.get('date', datetime.now().strftime("%Y-%m-%d"))
    
    try:
        all_shops_data = database.get_all_shops_summary(date)
        return jsonify(all_shops_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 错误处理
@app.errorhandler(404)
def page_not_found(e):
    return render_template('minimal_404.html'), 404

@app.errorhandler(500)
def internal_server_error(e):
    return render_template('minimal_500.html'), 500

# 初始化数据库路由
@app.route('/init_db')
def init_db():
    """初始化数据库"""
    try:
        database.init_database()
        return jsonify({'success': True, 'message': '数据库初始化成功'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # 确保模板目录存在
    os.makedirs('templates', exist_ok=True)
    
    # 初始化数据库
    try:
        database.init_database()
        print("✅ 数据库初始化成功")
    except Exception as e:
        print(f"⚠️  数据库初始化失败: {e}")
    
    print("🚀 启动维鲸运营系统Web版...")
    print("🌐 访问地址: http://localhost:5000")
    app.run(debug=True, port=5000)