import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import yfinance as yf
import warnings

# 경고 무시
warnings.filterwarnings("ignore")

# ==========================================
# 1. 수집할 종목 설정
# ==========================================
TARGET_STOCKS = [
    {"name": "삼성전자", "code": "005930", "ticker": "005930.KS"},
    {"name": "SK하이닉스", "code": "000660", "ticker": "000660.KS"},
    {"name": "카카오", "code": "035720", "ticker": "035720.KS"}
]

PAGES_TO_CRAWL = 2  # 종목당 수집할 페이지 수

# ==========================================
# 2. 네이버 금융 게시글 수집 (requests + Session)
# ==========================================
def get_headers():
    # 매번 조금씩 다른 브라우저인 척 위장
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
    ]
    return {
        'User-Agent': random.choice(user_agents),
        'Referer': 'https://finance.naver.com/',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Connection': 'keep-alive'
    }

def get_post_content(session, url):
    """
    세션(Session)을 유지하며 상세 페이지의 본문을 가져옵니다.
    """
    try:
        # 랜덤 딜레이 (필수)
        time.sleep(random.uniform(0.2, 0.5))
        
        res = session.get(url, headers=get_headers(), timeout=10)
        soup = BeautifulSoup(res.content, 'html.parser')
        
        content = ""
        
        # 본문 태그 찾기 (가장 확실한 순서대로)
        # 1. 스마트 에디터 (최신 글)
        if not content:
            smart_editor = soup.find('div', {'class': 'se-main-container'})
            if smart_editor:
                content = smart_editor.get_text(separator=" ", strip=True)

        # 2. 일반 HTML 본문 (구형) - id='body'가 가장 일반적
        if not content:
            body_tag = soup.find('div', {'id': 'body'})
            if body_tag:
                # 내부에 불필요한 스크립트나 스타일 제거
                for script in body_tag(['script', 'style']):
                    script.decompose()
                content = body_tag.get_text(separator=" ", strip=True)

        # 3. 구형 글 (scr01)
        if not content:
            scr01 = soup.find('div', {'class': 'scr01'})
            if scr01:
                content = scr01.get_text(separator=" ", strip=True)
                
        return content

    except Exception as e:
        return ""

def crawl_community(stocks, pages):
    all_data = []
    # 세션 시작 (쿠키 유지)
    session = requests.Session()
    
    for stock in stocks:
        name = stock['name']
        code = stock['code']
        print(f"\n=== [{name}] 게시글 수집 시작 ===")
        
        base_url = f"https://finance.naver.com/item/board.naver?code={code}&page="
        
        for page in range(1, pages + 1):
            url = base_url + str(page)
            print(f"  ▶ {page} 페이지 읽는 중...", end="")
            
            try:
                res = session.get(url, headers=get_headers())
                soup = BeautifulSoup(res.content, 'html.parser')
                table = soup.find('table', {'class': 'type2'})
                
                if not table:
                    print(" [차단 의심 혹은 데이터 없음]")
                    continue

                rows = table.find_all('tr')
                count = 0
                
                for row in rows:
                    if 'onmouseover' not in row.attrs:
                        continue
                        
                    cols = row.find_all('td')
                    if len(cols) < 6:
                        continue
                        
                    # 제목 링크 태그
                    link_tag = cols[1].find('a')
                    if not link_tag:
                        continue
                        
                    # 1. 전체 제목 가져오기 (title 속성 우선)
                    full_title = link_tag.get('title')
                    if not full_title:
                        full_title = link_tag.text.strip()
                        
                    # 2. 링크 생성
                    link_suffix = link_tag['href']
                    full_link = "https://finance.naver.com" + link_suffix
                    
                    # 3. 본문 가져오기 (함수 호출)
                    content = get_post_content(session, full_link)
                    
                    # 4. 기타 정보
                    date = cols[0].text.strip()
                    views = cols[3].text.strip()
                    
                    all_data.append({
                        'Date': date,
                        'Stock': name,
                        'Code': code,
                        'Title': full_title,
                        'Content': content,
                        'Link': full_link,
                        'Views': views
                    })
                    count += 1
                
                print(f" -> {count}개 완료")
                
            except Exception as e:
                print(f" [에러] {e}")
                continue
                
    return pd.DataFrame(all_data)

# ==========================================
# 3. 주가 데이터 수집 (yfinance)
# ==========================================
def get_stock_prices(stocks):
    all_prices = []
    print(f"\n=== 주가 데이터 수집 시작 (Yahoo Finance) ===")
    
    for stock in stocks:
        name = stock['name']
        ticker = stock['ticker']
        print(f"  ▶ [{name}] 데이터 요청 중...")
        
        try:
            # 최근 1달, 1시간 간격 데이터
            yf_stock = yf.Ticker(ticker)
            df = yf_stock.history(period="1mo", interval="60m")
            
            if df.empty:
                print("     -> 데이터 없음 (장 휴장일 등 확인 필요)")
                continue
                
            df.reset_index(inplace=True)
            df['Stock'] = name
            df['Code'] = stock['code']
            
            # 날짜 포맷 통일 (문자열로 변환)
            df['Datetime'] = df['Datetime'].astype(str)
            
            # 필요한 컬럼만 선택
            cols = ['Datetime', 'Stock', 'Code', 'Open', 'High', 'Low', 'Close', 'Volume']
            all_prices.append(df[cols])
            print(f"     -> {len(df)}개 행 수집 완료")
            
        except Exception as e:
            print(f"     -> [에러] {e}")
            
    if all_prices:
        return pd.concat(all_prices)
    else:
        return pd.DataFrame()

# ==========================================
# 4. 메인 실행 및 CSV 저장
# ==========================================
if __name__ == "__main__":
    # 1) 게시글 수집
    df_community = crawl_community(TARGET_STOCKS, PAGES_TO_CRAWL)
    if not df_community.empty:
        df_community.to_csv("stock_community_data.csv", index=False, encoding="utf-8-sig")
        print("\n✅ [성공] 게시글 데이터 저장 완료: stock_community_data.csv")
        # 미리보기
        print(df_community[['Stock', 'Title', 'Content']].head(3))
    else:
        print("\n❌ 게시글 수집 실패")

    # 2) 주가 데이터 수집
    df_price = get_stock_prices(TARGET_STOCKS)
    if not df_price.empty:
        df_price.to_csv("stock_price_data.csv", index=False, encoding="utf-8-sig")
        print("✅ [성공] 주가 데이터 저장 완료: stock_price_data.csv")
        # 미리보기
        print(df_price.head(3))
    else:
        print("❌ 주가 데이터 수집 실패")