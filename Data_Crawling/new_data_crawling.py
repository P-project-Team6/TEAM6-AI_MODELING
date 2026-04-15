import requests
from bs4 import BeautifulSoup
import pandas as pd
import yfinance as yf
import FinanceDataReader as fdr
import time
from tqdm import tqdm
import torch
from transformers import pipeline

# ==========================================
# 1. 설정 (Configuration)
# ==========================================
TOP_N = 80            # 상위 80개 기업
DEFAULT_PAGES = 15    # 기본 15페이지
HIGH_PAGES = 30       # 게시글 많은 종목(삼성전자, SK하이닉스)은 30페이지

# ==========================================
# 2. 비상용 하드코딩 리스트 (Fallback Data)
# API 통신 실패 시 사용할 대한민국 시가총액 상위 80개 우량주 리스트
# ==========================================
FALLBACK_TOP_80 = [
    {'Code': '005930', 'Name': '삼성전자'}, {'Code': '000660', 'Name': 'SK하이닉스'},
    {'Code': '373220', 'Name': 'LG에너지솔루션'}, {'Code': '207940', 'Name': '삼성바이오로직스'},
    {'Code': '005380', 'Name': '현대차'}, {'Code': '000270', 'Name': '기아'},
    {'Code': '068270', 'Name': '셀트리온'}, {'Code': '005490', 'Name': 'POSCO홀딩스'},
    {'Code': '035420', 'Name': 'NAVER'}, {'Code': '051910', 'Name': 'LG화학'},
    {'Code': '006400', 'Name': '삼성SDI'}, {'Code': '028260', 'Name': '삼성물산'},
    {'Code': '035720', 'Name': '카카오'}, {'Code': '105560', 'Name': 'KB금융'},
    {'Code': '012330', 'Name': '현대모비스'}, {'Code': '055550', 'Name': '신한지주'},
    {'Code': '066570', 'Name': 'LG전자'}, {'Code': '032830', 'Name': '삼성생명'},
    {'Code': '003670', 'Name': '포스코퓨처엠'}, {'Code': '086520', 'Name': '에코프로'},
    {'Code': '247540', 'Name': '에코프로비엠'}, {'Code': '015760', 'Name': '한국전력'},
    {'Code': '034020', 'Name': '두산에너빌리티'}, {'Code': '323410', 'Name': '카카오뱅크'},
    {'Code': '011200', 'Name': 'HMM'}, {'Code': '033180', 'Name': '메리츠금융지주'},
    {'Code': '018260', 'Name': '삼성SDS'}, {'Code': '086280', 'Name': '현대글로비스'},
    {'Code': '000810', 'Name': '삼성화재'}, {'Code': '096770', 'Name': 'SK이노베이션'},
    {'Code': '352820', 'Name': '하이브'}, {'Code': '316140', 'Name': '우리금융지주'},
    {'Code': '024110', 'Name': '기업은행'}, {'Code': '329180', 'Name': 'HD현대중공업'},
    {'Code': '010140', 'Name': '삼성중공업'}, {'Code': '012450', 'Name': '한화에어로스페이스'},
    {'Code': '009150', 'Name': '삼성전기'}, {'Code': '003550', 'Name': 'LG'},
    {'Code': '034730', 'Name': 'SK'}, {'Code': '005830', 'Name': 'DB손해보험'},
    {'Code': '042700', 'Name': '한미반도체'}, {'Code': '009540', 'Name': 'HD한국조선해양'},
    {'Code': '090430', 'Name': '아모레퍼시픽'}, {'Code': '010950', 'Name': 'S-Oil'},
    {'Code': '011170', 'Name': '롯데케미칼'}, {'Code': '018880', 'Name': '한온시스템'},
    {'Code': '030200', 'Name': 'KT'}, {'Code': '000210', 'Name': 'DL이앤씨'},
    {'Code': '004020', 'Name': '현대제철'}, {'Code': '032640', 'Name': 'LG유플러스'},
    {'Code': '017670', 'Name': 'SK텔레콤'}, {'Code': '028050', 'Name': '삼성엔지니어링'},
    {'Code': '000100', 'Name': '유한양행'}, {'Code': '047050', 'Name': '포스코인터내셔널'},
    {'Code': '259960', 'Name': '크래프톤'}, {'Code': '021240', 'Name': '코웨이'},
    {'Code': '112040', 'Name': '위메이드'}, {'Code': '011070', 'Name': 'LG이노텍'},
    {'Code': '006800', 'Name': '미래에셋증권'}, {'Code': '078930', 'Name': 'GS'},
    {'Code': '029780', 'Name': '삼성카드'}, {'Code': '010130', 'Name': '고려아연'},
    {'Code': '002790', 'Name': '아모레G'}, {'Code': '042660', 'Name': '한화오션'},
    {'Code': '161390', 'Name': '한국타이어앤테크놀로지'}, {'Code': '000720', 'Name': '현대건설'},
    {'Code': '047810', 'Name': '한국항공우주'}, {'Code': '307950', 'Name': '현대오토에버'},
    {'Code': '271560', 'Name': '오리온'}, {'Code': '064350', 'Name': '현대로템'},
    {'Code': '001450', 'Name': '현대해상'}, {'Code': '007070', 'Name': 'GS리테일'},
    {'Code': '028670', 'Name': '팬오션'}, {'Code': '139480', 'Name': '이마트'},
    {'Code': '012750', 'Name': '에스원'}, {'Code': '008930', 'Name': '한미약품'},
    {'Code': '069260', 'Name': '휴젤'}, {'Code': '282330', 'Name': 'BGF리테일'},
    {'Code': '004990', 'Name': '롯데지주'}, {'Code': '036570', 'Name': '엔씨소프트'}
]

# ==========================================
# 3. Zero-Shot 스팸 분류 모델 로드
# ==========================================
device = 0 if torch.cuda.is_available() else -1

print(">> Zero-Shot 스팸 분류 모델 로드 중...")
spam_classifier = pipeline(
    "zero-shot-classification", 
    model="MoritzLaurer/mDeBERTa-v3-base-mnli-xnli", 
    device=device
)

CANDIDATE_LABELS = ["주식 리딩방 유도", "불법 광고", "주식 토론 및 의견", "정보 공유"]

# ==========================================
# 4. 종목 리스트 확보 (API 통신 + Fallback 구조)
# ==========================================
def get_kr_top_stocks():
    print(f"\n>> 국내 시가총액 상위 {TOP_N}개 리스트 확보 중...")
    try:
        df_krx = fdr.StockListing('KRX')
        df_krx = df_krx.sort_values(by='Marcap', ascending=False).head(TOP_N)
        kr_stocks = df_krx[['Code', 'Name']].to_dict('records')
        print(f"   - API 통신 성공: {len(kr_stocks)}개 종목 확보 완료")
        return kr_stocks
    except Exception as e:
        print(f"   🚨 API 통신 실패 (사유: {e})")
        print("   >> [비상 시스템 가동] 미리 캐싱된 상위 80개 종목 리스트로 크롤링을 대체합니다!")
        # 에러가 나면 무조건 삼전 1개가 아니라, 80개 풀 리스트를 넘겨줌
        return FALLBACK_TOP_80

# ==========================================
# 5. 커뮤니티 데이터 수집 (의미론적 스팸 필터링 적용)
# ==========================================
def crawl_kr_community(stock_list):
    print(f"\n>> 국내 커뮤니티 데이터 수집 및 실시간 스팸 판별 시작...")
    
    results = []
    base_url = "https://finance.naver.com/item/board.naver"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

    for stock in tqdm(stock_list, desc="Community & Spam Check"):
        try:
            code = stock['Code']
            name = stock['Name']
            
            target_pages = HIGH_PAGES if code in ['005930', '000660'] else DEFAULT_PAGES
            
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
                        
                        title_text = link_tag.get_text(strip=True)
                        is_spam = False
                        
                        try:
                            classification_result = spam_classifier(title_text, CANDIDATE_LABELS)
                            top_label = classification_result['labels'][0]
                            top_score = classification_result['scores'][0]
                            
                            if top_label in ["주식 리딩방 유도", "불법 광고"] and top_score > 0.6:
                                is_spam = True
                        except:
                            pass 
                        
                        tds = row.find_all('td')
                        if len(tds) >= 6:
                            results.append({
                                'Date': tds[0].get_text(strip=True),
                                'Stock': name,
                                'Code': code,
                                'Type': 'Domestic',
                                'Title': title_text,
                                'Is_Spam': is_spam,
                                'Good': tds[4].get_text(strip=True),
                                'Bad': tds[5].get_text(strip=True),
                                'Views': tds[3].get_text(strip=True),
                                'Link': "https://finance.naver.com" + link_tag['href']
                            })
                time.sleep(0.05) 
        except Exception:
            continue

    return pd.DataFrame(results)

# ==========================================
# 6. 주가 데이터 수집
# ==========================================
def get_price_data(kr_stocks):
    print(f"\n>> 주가 데이터 수집 시작...")
    all_data = []

    for stock in tqdm(kr_stocks, desc="Price"):
        try:
            ticker = f"{stock['Code']}.KS"
            df = yf.download(ticker, period="7d", interval="1h", progress=False)
            
            if not df.empty:
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                
                df.reset_index(inplace=True)
                df = df.loc[:, ~df.columns.duplicated()]

                if 'Date' not in df.columns and 'index' in df.columns:
                    df.rename(columns={'index': 'Date'}, inplace=True)
                elif 'Datetime' in df.columns: 
                    df.rename(columns={'Datetime': 'Date'}, inplace=True)
                elif df.index.name == 'Date' or df.index.name == 'Datetime':
                    df.reset_index(inplace=True)
                    if 'index' in df.columns: df.rename(columns={'index': 'Date'}, inplace=True)

                df['Stock'] = stock['Name']
                df['Code'] = stock['Code']
                df['Type'] = 'Domestic'
                
                cols = ['Date', 'Stock', 'Code', 'Open', 'High', 'Low', 'Close', 'Volume']
                valid_cols = [c for c in cols if c in df.columns]
                all_data.append(df[valid_cols])
        except: continue

    if all_data:
        merged = pd.concat(all_data, ignore_index=True)
        return merged
    return pd.DataFrame()

# ==========================================
# 7. 실행 및 저장
# ==========================================
if __name__ == "__main__":
    kr_list = get_kr_top_stocks()
    
    df_comm = crawl_kr_community(kr_list)
    
    if not df_comm.empty:
        df_comm.to_csv("stock_community_data_top80.csv", index=False, encoding="utf-8-sig")
        print(f"✅ 커뮤니티 데이터 저장 완료: {len(df_comm)}건 (파일명: stock_community_data_top80.csv)")
    else:
        print("❌ 커뮤니티 데이터 수집 실패")

    df_price = get_price_data(kr_list)
    if not df_price.empty:
        df_price.to_csv("stock_price_data_top80.csv", index=False, encoding="utf-8-sig")
        print(f"✅ 주가 데이터 저장 완료: {len(df_price)}건 (파일명: stock_price_data_top80.csv)")
    else:
        print("❌ 주가 데이터 수집 실패")