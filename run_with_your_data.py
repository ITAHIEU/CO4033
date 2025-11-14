"""
Script để chạy IoT Power Pipeline với file CSV của bạn
"""

import pandas as pd
import sys
from pathlib import Path
from run_pandas import SimplePandaIPipeline

def main():
    print("🚀 IOT POWER PIPELINE - SỬ DỤNG DỮ LIỆU CỦA BẠN")
    print("="*60)
    
    # Kiểm tra có file dữ liệu không
    csv_file = Path("data/sample_power_data.csv")
    
    if not csv_file.exists():
        print("❌ Không tìm thấy file dữ liệu!")
        print("Vui lòng đặt file CSV vào:", csv_file)
        return
    
    # Load và kiểm tra dữ liệu
    try:
        df = pd.read_csv(csv_file)
        print(f"✅ Đã load file: {csv_file}")
        print(f"📊 Số dòng: {len(df):,}")
        print(f"📋 Các cột: {list(df.columns)}")
        
        # Kiểm tra các cột cần thiết
        required_cols = ['Date', 'Time', 'Global_active_power', 'Voltage']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            print(f"⚠️  Thiếu các cột: {missing_cols}")
            print("File cần có ít nhất: Date, Time, Global_active_power, Voltage")
            return
        
        print("\n📋 Preview dữ liệu:")
        print(df.head())
        
        # Chạy pipeline
        print(f"\n🔄 Bắt đầu xử lý pipeline...")
        pipeline = SimplePandaIPipeline()
        results = pipeline.run_full_pipeline(csv_file)
        
        print(f"\n✅ PIPELINE HOÀN THÀNH!")
        print(f"📊 Kết quả:")
        for key, value in results.items():
            print(f"   • {key}: {value:,}")
        
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        return

if __name__ == "__main__":
    main()