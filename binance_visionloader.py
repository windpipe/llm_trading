import os
import re
import pandas as pd
from datetime import datetime, timedelta

class BinanceVisionLoader:
    def __init__(self, data_folder='.//data_binance_vision//'):
        self.data_folder = data_folder
        self.loaded_data = {}

    def parse_filename(self, filename):
        """파일명 파싱 로직 개선"""
        pattern = r'([A-Z]+USDT)-(\d+[mh])-(\d{4}-\d{2}-\d{2})\.csv'
        match = re.match(pattern, filename)
        if match:
            return {
                'ticker': match.group(1),     # XRPUSDT
                'timeframe': match.group(2),  # 1m, 15m, 1h 등
                'date': datetime.strptime(match.group(3), '%Y-%m-%d').date(),
                'path': os.path.join(self.data_folder, filename)
            }
        return None

    def load_historical_data(self, days=3):
        """다중 시간대 데이터 로딩 시스템"""
        self.loaded_data = {}
        
        # 유효한 파일 목록 수집
        valid_files = []
        for fname in os.listdir(self.data_folder):
            file_info = self.parse_filename(fname)
            if file_info and fname.endswith('.csv'):
                valid_files.append(file_info)

        # 날짜별로 그룹화
        date_groups = {}
        for file in valid_files:
            key = (file['ticker'], file['timeframe'])
            if key not in date_groups:
                date_groups[key] = []
            date_groups[key].append(file)

        # 각 그룹별로 최신 N일 데이터 선택
        for (ticker, tf), files in date_groups.items():
            files.sort(key=lambda x: x['date'], reverse=True)
            selected_files = files[:days]
            
            # 데이터 병합
            dfs = []
            for f in selected_files:
                df = pd.read_csv(f['path'])
                df['open_time'] = pd.to_datetime(df['open_time'], unit='ms', utc=True)
                df['close_time'] = pd.to_datetime(df['close_time'], unit='ms', utc=True)
                dfs.append(df)
            
            if dfs:
                merged_df = pd.concat(dfs).sort_values('open_time')
                if ticker not in self.loaded_data:
                    self.loaded_data[ticker] = {}
                self.loaded_data[ticker][tf] = merged_df

    def get_available_data(self):
        """로드된 데이터 정보 요약"""
        summary = {}
        for ticker in self.loaded_data:
            summary[ticker] = {}
            for tf in self.loaded_data[ticker]:
                df = self.loaded_data[ticker][tf]
                summary[ticker][tf] = {
                    'start_date': df['open_time'].min().strftime('%Y-%m-%d'),
                    'end_date': df['open_time'].max().strftime('%Y-%m-%d'),
                    'rows': len(df),
                    'time_interval': f"{tf} => {pd.Timedelta(df['open_time'].diff().median())}"
                }
        return summary

# 사용 예시
if __name__ == "__main__":
    loader = BinanceVisionLoader()
    loader.load_historical_data(days=3)
    
    # 로드된 데이터 정보 출력
    summary = loader.get_available_data()
    for ticker in summary:
        print(f"\n[{ticker}]")
        for tf in summary[ticker]:
            info = summary[ticker][tf]
            print(f"• {tf} 데이터 ({info['rows']} rows)")
            print(f"  기간: {info['start_date']} ~ {info['end_date']}")
            print(f"  시간 간격: {info['time_interval']}")
