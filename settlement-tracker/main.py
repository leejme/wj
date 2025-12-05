#!/usr/bin/env python3
"""
维鲸运营系统 - 主程序入口
"""

import app  # 导入你的 app.py 模块
import pandas as pd
from datetime import datetime
import os

def show_menu():
    """显示主菜单"""
    print("\n" + "=" * 60)
    print("          维鲸运营系统 v1.0")
    print("=" * 60)
    print("1. 初始化/重置数据库")
    print("2. 查看数据库状态")
    print("3. 导入交易结算数据")
    print("4. 导入售后问题数据") 
    print("5. 导入发货明细数据")
    print("6. 查看日汇总")
    print("7. 查看月度汇总")
    print("8. 搜索订单")
    print("9. 搜索售后问题")
    print("10. 管理商品价格")
    print("11. 发货与结款对比分析")
    print("12. 清除所有数据")
    print("0. 退出")
    print("=" * 60)

def init_database():
    """初始化数据库"""
    print("\n正在初始化数据库...")
    app.init_database()
    print("✅ 数据库初始化完成！")
    
    # 显示店铺列表
    print("\n支持的店铺:")
    for i, shop in enumerate(app.SHOP_LIST, 1):
        shop_id = app.get_shop_id(shop)
        print(f"  {i:2d}. {shop} (ID: {shop_id})")

def show_database_status():
    """显示数据库状态"""
    print("\n📊 数据库状态:")
    debug_info = app.debug_data()
    
    status = {
        "售后问题表": debug_info['after_sales_count'],
        "交易结算表": debug_info['transaction_count'],
        "日汇总表": debug_info['summary_count'],
        "发货明细表": debug_info['shipping_count'],
        "商品价格表": debug_info['product_count']
    }
    
    for table, count in status.items():
        print(f"  {table}: {count} 条记录")
    
    # 显示示例数据
    if debug_info['sample_data']:
        print("\n📅 示例备货单号:")
        for stock_id, date in debug_info['sample_data'][:5]:
            print(f"  {stock_id} -> {date}")

def import_transaction_data():
    """导入交易结算数据"""
    print("\n📥 导入交易结算数据")
    print("请准备CSV文件，包含以下列:")
    print("  - 备货单号, SKU ID, 货品名称, 数量, 金额, 交易类型等")
    
    file_path = input("请输入CSV文件路径（直接回车使用示例数据）: ").strip()
    shop_name = select_shop()
    
    if not shop_name:
        return
    
    if file_path:
        if not os.path.exists(file_path):
            print(f"❌ 文件不存在: {file_path}")
            return
        try:
            df = pd.read_csv(file_path)
            print(f"✅ 成功读取 {len(df)} 行数据")
        except Exception as e:
            print(f"❌ 读取文件失败: {e}")
            return
    else:
        # 创建示例数据
        df = create_sample_transaction_data()
        print("📝 使用示例数据...")
    
    inserted, skipped = app.insert_transactions(df, shop_name)
    app.update_daily_summary(shop_name)
    app.update_all_shops_summary()
    
    print(f"\n✅ 导入完成:")
    print(f"  新增: {inserted} 条记录")
    print(f"  跳过（已存在）: {skipped} 条记录")

def import_after_sales_data():
    """导入售后问题数据"""
    print("\n📥 导入售后问题数据")
    shop_name = select_shop()
    
    if not shop_name:
        return
    
    # 创建示例数据
    df = create_sample_after_sales_data()
    
    inserted, skipped = app.insert_after_sales(df, shop_name)
    app.update_daily_summary(shop_name)
    app.update_all_shops_summary()
    
    print(f"\n✅ 导入完成:")
    print(f"  新增: {inserted} 条记录")
    print(f"  跳过（已存在）: {skipped} 条记录")

def import_shipping_details():
    """导入发货明细数据"""
    print("\n📦 导入发货明细数据")
    shop_name = select_shop()
    
    if not shop_name:
        return
    
    # 创建示例数据
    df = create_sample_shipping_data()
    
    inserted, skipped = app.insert_shipping_details(df, shop_name)
    
    print(f"\n✅ 导入完成:")
    print(f"  新增: {inserted} 条记录")
    print(f"  跳过（已存在）: {skipped} 条记录")

def view_daily_summary():
    """查看日汇总"""
    print("\n📅 查看日汇总")
    shop_name = select_shop()
    
    if not shop_name:
        return
    
    date = input("请输入日期 (格式: YYYY-MM-DD, 默认今天): ").strip()
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")
    
    summary = app.get_daily_summary(shop_name, date)
    
    if summary:
        print(f"\n📊 {shop_name} - {date} 日汇总:")
        print(f"  销售回款总额: ¥{summary['total_sales']:.2f}")
        print(f"  销售冲回总额: ¥{summary['total_refunds']:.2f}")
        print(f"  非商责补贴总额: ¥{summary['total_subsidies']:.2f}")
        print(f"  售后赔付总额: ¥{summary['total_after_sales']:.2f}")
        
        total_income = summary['total_sales'] + summary['total_subsidies']
        total_expense = summary['total_refunds'] + summary['total_after_sales']
        net_amount = total_income - total_expense
        
        print(f"\n  总收入: ¥{total_income:.2f}")
        print(f"  总支出: ¥{total_expense:.2f}")
        print(f"  净收入: ¥{net_amount:.2f}")
    else:
        print(f"❌ 没有找到 {date} 的数据")

def view_monthly_summary():
    """查看月度汇总"""
    print("\n📅 查看月度汇总")
    shop_name = select_shop()
    
    if not shop_name:
        return
    
    year = input("请输入年份 (默认今年): ").strip()
    month = input("请输入月份 (1-12): ").strip()
    
    if not year:
        year = datetime.now().year
    else:
        year = int(year)
    
    if not month:
        month = datetime.now().month
    else:
        month = int(month)
    
    summary = app.get_monthly_summary(shop_name, year, month)
    
    if summary and summary['daily_data']:
        print(f"\n📊 {shop_name} - {summary['month']} 月度汇总:")
        print("=" * 80)
        print(f"{'日期':12} {'销售回款':>12} {'销售冲回':>12} {'补贴':>12} {'售后赔付':>12} {'销售单数':>8}")
        print("-" * 80)
        
        for day in summary['daily_data']:
            print(f"{day['date']:12} "
                  f"¥{day['sales']:>11.2f} "
                  f"¥{day['refunds']:>11.2f} "
                  f"¥{day['subsidies']:>11.2f} "
                  f"¥{day['after_sales']:>11.2f} "
                  f"{day['sales_count']:>8d}")
        
        print("-" * 80)
        totals = summary['monthly_totals']
        print(f"{'月度总计':12} "
              f"¥{totals['sales']:>11.2f} "
              f"¥{totals['refunds']:>11.2f} "
              f"¥{totals['subsidies']:>11.2f} "
              f"¥{totals['after_sales']:>11.2f} "
              f"{totals['total_sales_count']:>8d}")
        
        print(f"\n📈 统计信息:")
        print(f"  总交易单数: {totals['total_transactions']}")
        print(f"  销售单数: {totals['total_sales_count']}")
        print(f"  退款单数: {totals['total_refunds_count']}")
        print(f"  补贴单数: {totals['total_subsidies_count']}")
        print(f"  售后单数: {totals['total_after_sales_count']}")
        
    else:
        print(f"❌ 没有找到 {year}-{month:02d} 的数据")

def search_orders():
    """搜索订单"""
    print("\n🔍 搜索订单")
    
    shop_name = select_shop(all_shops_option=True)
    stock_order_id = input("备货单号 (可选): ").strip()
    order_id = input("订单编号 (可选): ").strip()
    date = input("日期 (YYYY-MM-DD, 可选): ").strip()
    
    results = app.search_orders(
        shop_name=shop_name if shop_name != "所有店铺" else None,
        stock_order_id=stock_order_id,
        order_id=order_id,
        date=date
    )
    
    if len(results) > 0:
        print(f"\n✅ 找到 {len(results)} 条记录:")
        print(results.head(20).to_string())
        
        if len(results) > 20:
            print(f"\n... 还有 {len(results) - 20} 条记录未显示")
        
        # 保存到文件
        save = input("\n是否保存到CSV文件? (y/n): ").lower()
        if save == 'y':
            filename = f"订单搜索_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            results.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"✅ 已保存到: {filename}")
    else:
        print("❌ 没有找到匹配的记录")

def manage_product_prices():
    """管理商品价格"""
    print("\n💰 管理商品价格")
    
    shop_name = select_shop()
    if not shop_name:
        return
    
    print("\n1. 查看商品列表")
    print("2. 更新商品价格")
    choice = input("请选择 (1-2): ").strip()
    
    if choice == '1':
        # 查看商品列表
        spu_id = input("SPU ID (可选): ").strip() or None
        product_name = input("商品名称 (可选): ").strip() or None
        
        products = app.get_products(shop_name=shop_name, spu_id=spu_id, product_name=product_name)
        
        if len(products) > 0:
            print(f"\n✅ 找到 {len(products)} 个商品:")
            # 只显示关键列
            display_cols = ['spu_id', 'product_name', 'sku_attribute', 'unit_price', 'cost_price', 'total_sold']
            print(products[display_cols].head(20).to_string())
        else:
            print("❌ 没有找到商品")
    
    elif choice == '2':
        # 更新商品价格
        spu_id = input("SPU ID: ").strip()
        sku_attribute = input("SKU属性: ").strip()
        unit_price = float(input("销售单价: ").strip())
        cost_price = float(input("成本单价: ").strip())
        
        success = app.update_product_price(shop_name, spu_id, sku_attribute, unit_price, cost_price)
        if success:
            print("✅ 商品价格更新成功！")
        else:
            print("❌ 更新失败")

def compare_shipping_settlement():
    """发货与结款对比分析"""
    print("\n📊 发货与结款对比分析")
    
    shop_name = select_shop()
    if not shop_name:
        return
    
    start_date = input("开始日期 (YYYY-MM-DD, 可选): ").strip() or None
    end_date = input("结束日期 (YYYY-MM-DD, 可选): ").strip() or None
    
    results = app.compare_shipping_settlement(shop_name, start_date, end_date)
    
    if len(results) > 0:
        print(f"\n✅ 分析结果 ({len(results)} 条记录):")
        # 简化显示
        display_cols = ['stock_order_id', 'product_name', 'shipping_date', 
                       'shipping_amount', 'settlement_amount', 'settlement_rate']
        print(results[display_cols].head(20).to_string())
        
        # 统计信息
        total_shipping = results['shipping_amount'].sum()
        total_settlement = results['settlement_amount'].sum()
        overall_rate = (total_settlement / total_shipping * 100) if total_shipping > 0 else 0
        
        print(f"\n📈 总体统计:")
        print(f"  发货总金额: ¥{total_shipping:.2f}")
        print(f"  结款总金额: ¥{total_settlement:.2f}")
        print(f"  总体结款率: {overall_rate:.1f}%")
        
    else:
        print("❌ 没有找到发货记录")

def clear_all_data():
    """清除所有数据"""
    print("\n⚠️  警告: 这将删除所有数据！")
    confirm = input("确认清除所有数据? (输入 'YES' 确认): ")
    
    if confirm == 'YES':
        app.clear_all_data()
        print("✅ 所有数据已清除")
    else:
        print("❌ 操作已取消")

def select_shop(all_shops_option=False):
    """选择店铺"""
    print("\n🏪 选择店铺:")
    
    shops = app.SHOP_LIST.copy()
    if all_shops_option:
        shops.insert(0, "所有店铺")
    
    for i, shop in enumerate(shops, 1):
        print(f"  {i:2d}. {shop}")
    
    try:
        choice = int(input(f"请选择 (1-{len(shops)}): ").strip())
        if 1 <= choice <= len(shops):
            return shops[choice-1]
        else:
            print("❌ 选择无效")
            return None
    except ValueError:
        print("❌ 请输入数字")
        return None

def create_sample_transaction_data():
    """创建示例交易数据"""
    data = {
        '备货单号': ['WB2510162836467', 'WB2510162836468', 'WB2510171234567'],
        'SKU ID': ['SKU001', 'SKU002', 'SKU003'],
        '货品名称': ['商品A', '商品B', '商品C'],
        '数量': [1, 2, 1],
        '金额': [100.0, 200.0, 150.0],
        '交易类型': ['销售回款', '销售回款', '销售冲回'],
        '订单编号': ['ORD001', 'ORD002', 'ORD003'],
        '账务时间': ['2025-10-16 10:30:00', '2025-10-16 14:20:00', '2025-10-17 09:15:00']
    }
    return pd.DataFrame(data)

def create_sample_after_sales_data():
    """创建示例售后数据"""
    data = {
        '违规ID': ['VIOL001', 'VIOL002'],
        'SKU ID': ['SKU001', 'SKU002'],
        '货品名称': ['商品A', '商品B'],
        '赔付金额': [50.0, 30.0],
        '账务时间': ['2025-10-16 16:45:00', '2025-10-17 11:20:00']
    }
    return pd.DataFrame(data)

def create_sample_shipping_data():
    """创建示例发货数据"""
    data = {
        '备货单': ['WB2510162836467，1件', 'WB2510162836468，2件', 'WB2510171234567，1件'],
        '商品SPU ID': ['SPU001', 'SPU002', 'SPU003'],
        '商品SKC ID': ['SKC001', 'SKC002', 'SKC003'],
        '商品SKU ID': ['SKU001', 'SKU002', 'SKU003'],
        '商品名称': ['商品A', '商品B', '商品C'],
        '商品属性集': ['红色,M', '蓝色,L', '黑色,XL']
    }
    return pd.DataFrame(data)

def main():
    """主函数"""
    print("🚀 启动维鲸运营系统...")
    
    # 自动初始化数据库
    try:
        app.init_database()
        print("✅ 数据库连接正常")
    except Exception as e:
        print(f"❌ 数据库初始化失败: {e}")
        return
    
    while True:
        show_menu()
        
        try:
            choice = input("\n请选择功能 (0-12): ").strip()
            
            if choice == '0':
                print("\n感谢使用维鲸运营系统！再见！👋")
                break
            
            elif choice == '1':
                init_database()
            elif choice == '2':
                show_database_status()
            elif choice == '3':
                import_transaction_data()
            elif choice == '4':
                import_after_sales_data()
            elif choice == '5':
                import_shipping_details()
            elif choice == '6':
                view_daily_summary()
            elif choice == '7':
                view_monthly_summary()
            elif choice == '8':
                search_orders()
            elif choice == '9':
                print("搜索售后问题功能暂未实现")
            elif choice == '10':
                manage_product_prices()
            elif choice == '11':
                compare_shipping_settlement()
            elif choice == '12':
                clear_all_data()
            else:
                print("❌ 请选择有效的功能编号")
                
        except KeyboardInterrupt:
            print("\n\n操作中断")
            break
        except Exception as e:
            print(f"❌ 发生错误: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()