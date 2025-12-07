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
TOP_N = 80            # 상위 80개 기업
DEFAULT_PAGES = 15    # 기본 15페이지
HIGH_PAGES = 30       # 게시글 많은 종목(삼성전자, SK하이닉스)은 30페이지

# ==========================================
# 2. 종목 리스트 확보 (국내 Top 80)
# ==========================================
def get_kr_top_stocks():
    print(f">> 국내 시가총액 상위 {TOP_N}개 리스트 확보 중...")
    try:
        # KRX 전체 상장 종목 -> 시가총액 순 정렬 -> 상위 N개
        df_krx = fdr.StockListing('KRX')
        df_krx = df_krx.sort_values(by='Marcap', ascending=False).head(TOP_N)
        kr_stocks = df_krx[['Code', 'Name']].to_dict('records')
        print(f"   - 확보 완료: {len(kr_stocks)}개 종목")
        return kr_stocks
    except Exception as e:
        print(f"!! 리스트 확보 실패: {e}")
        return [{'Code': '005930', 'Name': '삼성전자'}]

# ==========================================
# 3. 커뮤니티 데이터 수집 (삼성전자, SK하이닉스 40p)
# ==========================================
def crawl_kr_community(stock_list):
    print(f"\n>> 국내 커뮤니티 데이터 수집 시작...")
    print(f"   - 삼성전자/SK하이닉스: {HIGH_PAGES}페이지 | 그 외: {DEFAULT_PAGES}페이지")
    
    results = []
    base_url = "https://finance.naver.com/item/board.naver"
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

    for stock in tqdm(stock_list, desc="Community"):
        try:
            code = stock['Code']
            name = stock['Name']
            
            # [핵심 수정] 삼성전자(005930)와 SK하이닉스(000660)는 40페이지 수집
            if code in ['005930', '000660']:
                target_pages = HIGH_PAGES
            else:
                target_pages = DEFAULT_PAGES
            
            for page in range(1, target_pages + 1):
                resp = requests.get(f"{base_url}?code={code}&page={page}", headers=headers, timeout=5)
                soup = BeautifulSoup(resp.text, 'html.parser')
                table = soup.find('table', {'class': 'type2'})
                if not table: continue
                
                for row in table.find_all('tr'):
                    title_td = row.find('td', {'class': 'title'})
                    if title_td:
                        link_tag = title_td.find('a')
                        if not link_tag: continue
                        
                        tds = row.find_all('td')
                        if len(tds) >= 6:
                            results.append({
                                'Date': tds[0].get_text(strip=True),
                                'Stock': name,
                                'Code': code,
                                'Type': 'Domestic',
                                'Title': link_tag.get_text(strip=True),
                                'Good': tds[4].get_text(strip=True),
                                'Bad': tds[5].get_text(strip=True),
                                'Views': tds[3].get_text(strip=True),
                                'Link': "https://finance.naver.com" + link_tag['href']
                            })
                time.sleep(0.05) # 차단 방지용 미세 딜레이
        except Exception:
            continue

    return pd.DataFrame(results)

# ==========================================
# 4. 주가 데이터 수집 (기존 로직 유지)
# ==========================================
def get_price_data(kr_stocks):
    print(f"\n>> 주가 데이터 수집 시작...")
    all_data = []

    for stock in tqdm(kr_stocks, desc="Price"):
        try:
            ticker = f"{stock['Code']}.KS"
            # 7일치, 1시간 간격 데이터 (정확도 산출용)
            df = yf.download(ticker, period="7d", interval="1h", progress=False)
            
            if not df.empty:
                # 컬럼 정리
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                
                df.reset_index(inplace=True)
                df = df.loc[:, ~df.columns.duplicated()] # 중복 컬럼 제거

                # 날짜 컬럼명 통일
                if 'Date' not in df.columns and 'index' in df.columns:
                    df.rename(columns={'index': 'Date'}, inplace=True)
                elif 'Datetime' in df.columns: 
                    df.rename(columns={'Datetime': 'Date'}, inplace=True)
                elif df.index.name == 'Date' or df.index.name == 'Datetime':
                    df.reset_index(inplace=True) # 인덱스가 Date인 경우
                    if 'index' in df.columns: df.rename(columns={'index': 'Date'}, inplace=True)

                df['Stock'] = stock['Name']
                df['Code'] = stock['Code']
                df['Type'] = 'Domestic'
                
                # 필요한 컬럼만 추출
                cols = ['Date', 'Stock', 'Code', 'Open', 'High', 'Low', 'Close', 'Volume']
                valid_cols = [c for c in cols if c in df.columns]
                all_data.append(df[valid_cols])
        except: continue

    if all_data:
        merged = pd.concat(all_data, ignore_index=True)
        return merged
    
    return pd.DataFrame()

# ==========================================
# 5. 실행 및 저장
# ==========================================
if __name__ == "__main__":
    # 1. 리스트 확보 (Top 80)
    kr_list = get_kr_top_stocks()
    
    # 2. 커뮤니티 데이터 (삼성전자/하이닉스 40p, 나머지 20p)
    df_comm = crawl_kr_community(kr_list)
    
    if not df_comm.empty:
        df_comm.to_csv("stock_community_data_top80.csv", index=False, encoding="utf-8-sig")
        print(f"✅ 커뮤니티 데이터 저장 완료: {len(df_comm)}건 (파일명: stock_community_data_top80.csv)")
    else:
        print("❌ 커뮤니티 데이터 수집 실패")

    # 3. 주가 데이터 (Top 80)
    df_price = get_price_data(kr_list)
    if not df_price.empty:
        df_price.to_csv("stock_price_data_top80.csv", index=False, encoding="utf-8-sig")
        print(f"✅ 주가 데이터 저장 완료: {len(df_price)}건 (파일명: stock_price_data_top80.csv)")
    else:
        print("❌ 주가 데이터 수집 실패")