# Hướng dẫn Trực quan hóa IoT Data bằng Power BI

## 📊 Dữ liệu đã sẵn sàng

Pipeline IoT đã xử lý thành công dữ liệu và tạo ra các file CSV trong thư mục `powerbi_data`:

### 📁 Các file dữ liệu:
- **bronze_raw_data.csv** (2,075,259 dòng): Dữ liệu thô từ sensors
- **silver_clean_data.csv** (2,075,259 dòng): Dữ liệu đã làm sạch và chuẩn hóa
- **gold_daily_aggregates.csv** (1,442 dòng): Tổng hợp theo ngày
- **gold_hourly_aggregates.csv** (34,589 dòng): Tổng hợp theo giờ
- **forecast_data.csv** (7 dòng): Dự báo tiêu thụ điện

## 🚀 Hướng dẫn từng bước

### Bước 1: Mở Power BI Desktop
1. Khởi chạy Power BI Desktop
2. Chọn "Get Data" từ Home ribbon
3. Chọn "Text/CSV"

### Bước 2: Import dữ liệu
1. Navigate đến: `E:\Tai lieu\Dữ liệu lớn và trí tuệ kinh doanh\Iot (1)\powerbi_data`
2. Chọn file bạn muốn import (khuyến nghị bắt đầu với `gold_daily_aggregates.csv`)
3. Preview dữ liệu và click "Load"

### Bước 3: Tạo Relationships (nếu import nhiều file)
1. Vào Model view
2. Tạo relationships giữa các bảng dựa trên:
   - Date fields
   - Device IDs
   - Location fields

## 📈 Các biểu đồ khuyến nghị

### 1. Dashboard Overview
- **Line Chart**: Xu hướng tiêu thủ điện theo thời gian
- **Card Visual**: Tổng năng lượng tiêu thụ, số thiết bị, peak consumption
- **Gauge Chart**: Hiệu suất hệ thống

### 2. Time Series Analysis
- **Line Chart với Multiple Series**: So sánh consumption patterns
- **Area Chart**: Tích lũy năng lượng theo thời gian
- **Calendar Heat Map**: Mẫu hình tiêu thụ theo ngày/tuần

### 3. Device Performance
- **Bar Chart**: Top thiết bị tiêu thụ nhiều nhất
- **Scatter Plot**: Correlation giữa các metrics
- **Tree Map**: Phân bố năng lượng theo device/location

### 4. Forecasting Dashboard
- **Line Chart**: So sánh actual vs predicted values
- **Waterfall Chart**: Contribution factors
- **KPI Cards**: Forecast accuracy metrics

## 🎯 Measures và Calculations quan trọng

### DAX Measures cần tạo:
```dax
// Total Energy Consumption
Total Energy = SUM('gold_daily_aggregates'[energy_kWh])

// Average Daily Consumption
Avg Daily = AVERAGE('gold_daily_aggregates'[energy_kWh])

// Peak Hour Consumption
Peak Hour = MAX('gold_hourly_aggregates'[avg_power_kW])

// Energy Efficiency
Efficiency = DIVIDE([Total Energy], [Peak Hour], 0)

// Month over Month Growth
MoM Growth = 
VAR CurrentMonth = [Total Energy]
VAR PreviousMonth = CALCULATE([Total Energy], DATEADD('Date'[Date], -1, MONTH))
RETURN DIVIDE(CurrentMonth - PreviousMonth, PreviousMonth, 0)
```

## 🔍 Filters và Slicers

### Thêm các Slicers:
- **Date Range Slicer**: Lọc theo khoảng thời gian
- **Device Type**: Lọc theo loại thiết bị
- **Location**: Lọc theo vị trí
- **Hour of Day**: Phân tích theo giờ trong ngày

## 📊 Template Dashboard Layout

```
+-------------------+-------------------+-------------------+
|   Total Energy    |   Avg Daily       |   Peak Hour       |
|   [KPI Card]      |   [KPI Card]      |   [KPI Card]      |
+-------------------+-------------------+-------------------+
|                                                           |
|           Energy Consumption Trend                        |
|              [Line Chart]                                 |
|                                                           |
+---------------------------+-------------------------------+
|                           |                               |
|   Top Devices             |    Hourly Pattern            |
|   [Bar Chart]             |    [Heat Map]                |
|                           |                               |
+---------------------------+-------------------------------+
|                                                           |
|           Forecast vs Actual                              |
|              [Line Chart]                                 |
|                                                           |
+-----------------------------------------------------------+
```

## 🎨 Formatting Tips

### Color Scheme (IoT Theme):
- Primary: #2E86AB (Blue)
- Secondary: #A23B72 (Purple)
- Accent: #F18F01 (Orange)
- Background: #C73E1D (Red)
- Text: #333333 (Dark Gray)

### Best Practices:
1. Sử dụng consistent color palette
2. Add tooltips với detailed information
3. Enable drill-through cho detailed analysis
4. Tạo bookmarks cho different views
5. Publish to Power BI Service để chia sẻ

## 📱 Mobile Optimization

1. Tạo Mobile layout trong Power BI Desktop
2. Sắp xếp visuals theo priority
3. Sử dụng simple charts cho mobile
4. Test trên Power BI Mobile app

## 🔄 Refresh Schedule

Sau khi publish lên Power BI Service:
1. Setup automated refresh schedule
2. Configure data source credentials
3. Set up alerts cho abnormal patterns
4. Enable automatic email reports

## 🚨 Monitoring và Alerts

Tạo alerts cho:
- Energy consumption spikes
- Device performance issues
- Forecast accuracy deviations
- System anomalies

---

**✅ Kết quả**: Bạn sẽ có một dashboard interactive với khả năng:
- Theo dõi real-time energy consumption
- Phân tích patterns và trends
- Dự báo consumption
- Optimize device performance