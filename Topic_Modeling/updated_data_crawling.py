import requests
from bs4 import BeautifulSoup
import pandas as pd
import yfinance as yf
import FinanceDataReader as fdr
import time
from tqdm import tqdm

# ==========================================
# 1. 설정 (Configuration)
# ==========================================
TOP_N = 80
DEFAULT_PAGES = 15
HIGH_PAGES = 30


# ==========================================
# 2. 종목 리스트 확보 (국내 Top 80)
# ==========================================
def get_kr_top_stocks():
    print(f">> 국내 시가총액 상위 {TOP_N}개 리스트 확보 중...")
    try:
        df_krx = fdr.StockListing('KRX')
        df_krx = df_krx.sort_values(by='Marcap', ascending=False).head(TOP_N)
        return df_krx[['Code', 'Name']].to_dict('records')
    except Exception as e:
        print(f"!! 리스트 확보 실패: {e}")
        return [{'Code': '005930', 'Name': '삼성전자'}]


# ==========================================
# 3. 커뮤니티 데이터 수집
#    → 날짜/시간 분리 기능을 함수 내부에 직접 포함
# ==========================================
def crawl_kr_community(stock_list):
    print(f"\n>> 국내 커뮤니티 데이터 수집 시작...")
    print(f"   - 삼성전자/SK하이닉스: {HIGH_PAGES}페이지 | 그 외: {DEFAULT_PAGES}페이지")

    results = []
    base_url = "https://finance.naver.com/item/board.naver"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    for stock in tqdm(stock_list, desc="Community"):
        code = stock['Code']
        name = stock['Name']

        target_pages = HIGH_PAGES if code in ['005930', '000660'] else DEFAULT_PAGES

        for page in range(1, target_pages + 1):
            try:
                resp = requests.get(f"{base_url}?code={code}&page={page}",
                                    headers=headers, timeout=5)
                soup = BeautifulSoup(resp.text, 'html.parser')
                table = soup.find('table', {'class': 'type2'})
                if not table:
                    continue

                for row in table.find_all('tr'):
                    title_td = row.find('td', {'class': 'title'})
                    if not title_td:
                        continue

                    link_tag = title_td.find('a')
                    if not link_tag:
                        continue

                    tds = row.find_all('td')
                    if len(tds) < 6:
                        continue

                    # ================================
                    # 날짜/시간 분리 (함수 내부에서 처리)
                    # ================================
                    raw_datetime = tds[0].get_text(strip=True)
                    dt = pd.to_datetime(raw_datetime, errors='coerce')

                    date_only = dt.date().isoformat() if pd.notnull(dt) else None
                    time_only = dt.time().isoformat() if pd.notnull(dt) else None

                    results.append({
                        'Date': date_only,       
                        'Time': time_only,          
                        'Stock': name,
                        'Code': code,                        
                        'Title': link_tag.get_text(strip=True),
                        'Good': tds[4].get_text(strip=True),
                        'Bad': tds[5].get_text(strip=True),
                        'Views': tds[3].get_text(strip=True),
                        'Link': "https://finance.naver.com" + link_tag['href']
                    })
            except Exception:
                continue

            time.sleep(0.05)

    return pd.DataFrame(results)


# ==========================================
# 4. 주가 데이터 수집
# ==========================================
def get_price_data(kr_stocks):
    print(f"\n>> 주가 데이터 수집 시작...")
    all_data = []

    for stock in tqdm(kr_stocks, desc="Price"):
        try:
            ticker = f"{stock['Code']}.KS"
            df = yf.download(ticker, period="7d", interval="1h", progress=False)

            if df.empty:
                continue

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            df.reset_index(inplace=True)
            df = df.loc[:, ~df.columns.duplicated()]

            if 'Date' not in df.columns and 'index' in df.columns:
                df.rename(columns={'index': 'Date'}, inplace=True)
            elif 'Datetime' in df.columns:
                df.rename(columns={'Datetime': 'Date'}, inplace=True)
            elif df.index.name in ['Date', 'Datetime']:
                df.reset_index(inplace=True)
                if 'index' in df.columns:
                    df.rename(columns={'index': 'Date'}, inplace=True)

            df['Stock'] = stock['Name']
            df['Code'] = stock['Code']
            
            cols = ['Date', 'Stock', 'Code', 'Open', 'High', 'Low', 'Close', 'Volume']
            valid_cols = [c for c in cols if c in df.columns]
            all_data.append(df[valid_cols])

        except:
            continue

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


# ==========================================
# 5. 실행 및 저장
# ==========================================
if __name__ == "__main__":
    kr_list = get_kr_top_stocks()
    
    df_comm = crawl_kr_community(kr_list)
    if not df_comm.empty:
        df_comm.to_csv("stock_community_data_top80.csv",
                       index=False, encoding="utf-8-sig")
        print(f"커뮤니티 데이터 저장 완료: {len(df_comm)}건")
    else:
        print("커뮤니티 데이터 수집 실패")

    df_price = get_price_data(kr_list)
    if not df_price.empty:
        df_price.to_csv("stock_price_data_top80.csv",
                        index=False, encoding="utf-8-sig")
        print(f"주가 데이터 저장 완료: {len(df_price)}건")
    else:
        print("주가 데이터 수집 실패")
