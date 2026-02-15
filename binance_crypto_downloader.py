#!/usr/bin/env python3
"""
Binance 加密货币数据下载工具 - 改进版
增强网络容错和重试机制
"""

import ccxt
import pandas as pd
from datetime import datetime
import time
import os


class BinanceCryptoDownloaderV2:
    """改进版 Binance 下载器 - 增强网络容错"""
    
    def __init__(self):
        """初始化下载器"""
        # 💡 提示：请确保你的 VPN 开启了本地端口（通常是 7890 或 1080）
        proxy_url = 'http://127.0.0.1:7890' 
        
        self.exchange_options = [
            {
                'name': 'Binance 直连 (无代理)',
                'config': {
                    'enableRateLimit': True,
                    'timeout': 30000,
                    'options': {'defaultType': 'spot'},
                }
            },
            {
                'name': 'Binance 代理模式 (端口 7890)',
                'config': {
                    'enableRateLimit': True,
                    'timeout': 30000,
                    'options': {'defaultType': 'spot'},
                    # ✅ 修复：必须是字典格式
                    'proxies': {
                        'http': proxy_url,
                        'https': proxy_url,
                    },
                }
            }
        ]
        
        self.exchange = None
        self._init_exchange()
        
        self.data_dir = 'trading_data/raw'
        os.makedirs(self.data_dir, exist_ok=True)
    
    def _init_exchange(self):
        """初始化交易所连接"""
        for option in self.exchange_options:
            try:
                print(f"尝试连接 {option['name']}...")
                self.exchange = ccxt.binance(option['config'])
                
                # 测试连接
                self.exchange.fetch_ticker('BTC/USDT')
                print(f"✅ {option['name']} 连接成功\n")
                return
            except Exception as e:
                print(f"❌ {option['name']} 连接失败: {str(e)[:50]}...")
                continue
        
        print("\n⚠️ 所有连接方式都失败了")
        print("请检查网络连接或稍后重试")
        exit(1)
    
    def download_crypto(self, symbol, start_date, end_date=None, timeframe='4h', max_retries=3):
        """
        下载加密货币数据（增强容错）
        """
        print(f"\n{'='*60}")
        print(f"📊 下载 {symbol} 数据")
        print(f"{'='*60}")
        
        for attempt in range(max_retries):
            try:
                return self._download_with_retry(symbol, start_date, end_date, timeframe)
            except Exception as e:
                print(f"❌ 尝试 {attempt + 1}/{max_retries} 失败: {str(e)[:80]}")
                
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5
                    print(f"⏳ 等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    print(f"❌ {symbol} 下载失败，已达最大重试次数")
                    return None
    
    def _download_with_retry(self, symbol, start_date, end_date, timeframe):
        """实际下载逻辑"""
        since = self.exchange.parse8601(f'{start_date}T00:00:00Z')
        
        if end_date:
            end_ts = self.exchange.parse8601(f'{end_date}T23:59:59Z')
        else:
            end_ts = None
        
        all_ohlcv = []
        batch_count = 0
        consecutive_errors = 0
        
        while True:
            try:
                # 获取数据
                ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, since, limit=1000)
                
                if not ohlcv:
                    break
                
                all_ohlcv.extend(ohlcv)
                batch_count += 1
                consecutive_errors = 0  # 重置错误计数
                
                # 更新时间
                since = ohlcv[-1][0] + 1
                
                if end_ts and since >= end_ts:
                    break
                
                if len(ohlcv) < 1000:
                    break
                
                # 显示进度
                print(f"  已下载 {len(all_ohlcv)} 根K线（批次 {batch_count}）", end='\r')
                
                # 短暂延迟
                time.sleep(0.2)
                
            except Exception as e:
                consecutive_errors += 1
                
                if consecutive_errors > 3:
                    raise Exception(f"连续错误过多: {e}")
                
                print(f"\n  ⚠️ 临时错误，继续重试... ({consecutive_errors}/3)")
                time.sleep(2)
                continue
        
        print()  # 换行
        
        if not all_ohlcv:
            return None
        
        # 转换为 DataFrame
        df = pd.DataFrame(
            all_ohlcv,
            columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
        )
        df['time'] = pd.to_datetime(df['timestamp'], unit='ms')
        df = df[['time', 'open', 'high', 'low', 'close', 'volume']]
        df = df.sort_values('time').reset_index(drop=True)
        
        print(f"✅ 下载完成!")
        print(f"   总K线数: {len(df)}")
        print(f"   时间范围: {df['time'].min()} 至 {df['time'].max()}")
        
        return df
    
    def save_data(self, df, filename):
        """保存数据"""
        if df is None or len(df) == 0:
            return False
        
        filepath = f"{self.data_dir}/{filename}"
        df.to_csv(filepath, index=False)
        
        file_size = os.path.getsize(filepath) / 1024
        print(f"✅ 数据已保存: {filepath}")
        print(f"   文件大小: {file_size:.2f} KB")
        
        return True
    
    def download_and_save(self, symbol, start_date, end_date=None, timeframe='4h'):
        """一键下载并保存"""
        df = self.download_crypto(symbol, start_date, end_date, timeframe)
        
        if df is None:
            return False
        
        coin_name = symbol.replace('/', '')
        date_str = datetime.now().strftime('%Y%m%d')
        filename = f"{coin_name}_{timeframe.upper()}_Binance_{date_str}.csv"
        
        return self.save_data(df, filename)
    
    def batch_download(self, symbols, start_date, end_date=None, timeframe='4h'):
        """批量下载"""
        print(f"\n{'='*60}")
        print(f"批量下载 {len(symbols)} 个加密货币")
        print(f"{'='*60}")
        
        results = []
        
        for i, symbol in enumerate(symbols, 1):
            print(f"\n[{i}/{len(symbols)}] 正在处理 {symbol}")
            print("-" * 60)
            
            success = self.download_and_save(symbol, start_date, end_date, timeframe)
            
            results.append({
                'symbol': symbol,
                'status': 'success' if success else 'failed'
            })
            
            if i < len(symbols):
                print(f"\n⏸️ 准备下载下一个币种...")
                time.sleep(2)
        
        # 汇总
        print(f"\n{'='*60}")
        print("下载汇总")
        print(f"{'='*60}")
        
        success_count = sum(1 for r in results if r['status'] == 'success')
        print(f"\n✅ 成功: {success_count}/{len(results)}")
        print(f"❌ 失败: {len(results) - success_count}/{len(results)}")
        
        print("\n详细结果:")
        for result in results:
            emoji = "✅" if result['status'] == 'success' else "❌"
            print(f"{emoji} {result['symbol']}: {result['status']}")
        
        return results


def main():
    """主函数"""
    print("="*60)
    print("🚀 Binance 加密货币数据下载工具 v2")
    print("="*60)
    print("\n增强版功能:")
    print("✅ 自动重试机制")
    print("✅ 网络容错")
    print("✅ 多种连接方式")
    print()
    
    # 初始化
    downloader = BinanceCryptoDownloaderV2()
    
    # 目标币种
    target_symbols = [
        'BTC/USDT',
        'ETH/USDT',
        'XRP/USDT',
        'SOL/USDT',
        'DOGE/USDT',
    ]
    
    print("\n目标币种: BTC, ETH, XRP, SOL, DOGE")
    
    # 时间范围（使用更保守的设置）
    print("\n推荐时间范围:")
    print("1. 最近 1 年 - 快速下载 ⭐")
    print("2. 最近 2 年 - 推荐")
    print("3. 最近 3 年 - 完整数据")
    
    choice = input("\n选择 (1-3, 默认=1): ").strip() or '1'
    
    today = datetime.now().strftime('%Y-%m-%d')
    
    if choice == '1':
        start_date = '2024-02-15'
    elif choice == '2':
        start_date = '2023-02-15'
    else:
        start_date = '2022-02-15'
    
    end_date = today
    
    print(f"\n时间范围: {start_date} 至 {end_date}")
    print("时间周期: 4h")
    
    confirm = input("\n确认开始下载? (y/n): ").strip().lower()
    
    if confirm != 'y':
        print("已取消")
        return
    
    # 开始下载
    start_time = time.time()
    results = downloader.batch_download(target_symbols, start_date, end_date, '4h')
    elapsed = time.time() - start_time
    
    print(f"\n{'='*60}")
    print("✅ 全部完成！")
    print(f"{'='*60}")
    print(f"\n总用时: {elapsed:.1f} 秒")
    print(f"数据保存在: {downloader.data_dir}/")


if __name__ == '__main__':
    try:
        import ccxt
        import pandas as pd
    except ImportError as e:
        print(f"❌ 缺少依赖: {e}")
        print("请运行: pip install ccxt pandas")
        exit(1)
    
    main()