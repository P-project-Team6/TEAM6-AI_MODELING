import pandas as pd
import numpy as np
import warnings
import os

# 경고 메시지 무시 (깔끔한 출력을 위해)
warnings.filterwarnings('ignore')

# ==========================================
# 1. 설정 (Configuration)
# ==========================================
ROLL_WINDOW = 7               # 7일 이동평균 기간
MIN_POSTS = 3                 # 최소 게시물 수 (노이즈 제거 기준)
W_DAILY = 0.6                 # 일간 증가율 가중치
W_WEEKLY = 0.4                # 7일 평균 대비 증가율 가중치

INPUT_FILE = "stock_community_labeled.csv"
OUTPUT_ALL_FILE = "daily_mentions_results.csv"      # [파일 1] 전체 데이터 및 통계 기록
OUTPUT_TOP_FILE = "top10_increasing_stocks.csv"     # [파일 2] 급등 테마 TOP 10

# ==========================================
# 2. 파일 로드 안전 함수
# ==========================================
def load_csv_safe(filepath):
    encodings = ['utf-8', 'utf-8-sig', 'cp949', 'euc-kr']
    for enc in encodings:
        try:
            return pd.read_csv(filepath, encoding=enc)
        except (UnicodeDecodeError, FileNotFoundError):
            continue
    print(f"❌ 파일을 열 수 없습니다: {filepath}")
    return None

# ==========================================
# 3. 안전 로그 함수
# ==========================================
def safe_log(x):
    return np.log1p(np.maximum(x, 0))

# ==========================================
# 4. 데이터 로드 및 전처리 (스팸 필터링 적용)
# ==========================================
def load_and_preprocess(path):
    print(f">> [{path}] 데이터 로딩 및 전처리 시작...")
    df = load_csv_safe(path)
    if df is None or df.empty: return pd.DataFrame()

    # 종목 코드 6자리 통일 (005930 등)
    df['Code'] = df['Code'].astype(str).str.zfill(6)

    # 날짜 처리 (시간 제외, 날짜만 추출)
    df['Date'] = pd.to_datetime(df['Date'], format='%Y.%m.%d %H:%M', errors='coerce').dt.date
    df = df.dropna(subset=['Date'])

    # [핵심] 스팸 데이터 제외: 여론 조작 방지
    if 'Is_Spam' in df.columns:
        initial_len = len(df)
        df = df[df['Is_Spam'] == False]
        print(f"   - 스팸 필터링: {initial_len}건 -> {len(df)}건 (순수 여론만 집계)")

    # 반응도 관련 수치형 변환
    for col in ['Good', 'Bad', 'Views']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    # 개별 게시글의 반응도 산출
    df['engagement'] = df['Good'] + df['Bad'] + safe_log(df['Views'])

    return df

# ==========================================
# 5. 시계열 추세 분석 알고리즘 (성장률 및 인기도 계산)
# ==========================================
def analyze_trends(df):
    # 1. 일별/종목별 집계 
    daily = df.groupby(['Date', 'Code', 'Stock']).agg(
        post_count=('Title', 'count'),
        engagement_sum=('engagement', 'sum')
    ).reset_index()

    daily = daily.sort_values(['Code', 'Date'])

    # 2. 7일 이동평균 (게시글 수 기준)
    daily['post_count_7d_avg'] = daily.groupby('Code')['post_count'].transform(
        lambda x: x.rolling(window=ROLL_WINDOW, min_periods=1).mean().shift(1)
    ).fillna(0)

    # 3. 전일 게시글 수
    daily['post_count_prev'] = daily.groupby('Code')['post_count'].shift(1).fillna(0)

    # 4. 성장률 산출 (daily_growth, weekly_growth)
    daily['daily_growth'] = np.where(
        daily['post_count_prev'] > 0, 
        (daily['post_count'] - daily['post_count_prev']) / daily['post_count_prev'], 
        0
    )
    daily['weekly_growth'] = np.where(
        daily['post_count_7d_avg'] > 0, 
        (daily['post_count'] - daily['post_count_7d_avg']) / daily['post_count_7d_avg'], 
        0
    )

    # 5. 최종 가중 성장률 (growth_rate) - 급등 테마 포착 핵심 지표
    daily['growth_rate'] = (daily['daily_growth'] * W_DAILY) + (daily['weekly_growth'] * W_WEEKLY)

    # 6. 반응도 정규화 (norm_engagement)
    eng_min, eng_max = daily['engagement_sum'].min(), daily['engagement_sum'].max()
    daily['norm_engagement'] = (daily['engagement_sum'] - eng_min) / (eng_max - eng_min) if eng_max > eng_min else 0
    
    # 7. 최종 인기도 산출 (popularity)
    daily['popularity'] = daily['norm_engagement'] * (1 + daily['growth_rate'])
    
    # 8. 유효 필터 (최소 게시글 수 미달 종목 체크)
    daily['valid'] = daily['post_count'] >= MIN_POSTS

    # 결과물 컬럼 순서 깔끔하게 정리
    result_cols = [
        'Date', 'Stock', 'Code', 'post_count', 'engagement_sum', 
        'daily_growth', 'weekly_growth', 'growth_rate', 
        'norm_engagement', 'popularity', 'valid'
    ]
    return daily[result_cols]

# ==========================================
# 6. 메인 실행 및 파일 저장
# ==========================================
if __name__ == "__main__":
    print(">> 테마 인기도 분석 파이프라인 가동...")
    
    raw_data = load_and_preprocess(INPUT_FILE)
    
    if not raw_data.empty:
        result_df = analyze_trends(raw_data)
        
        # [파일 1] 전체 데이터 결과 저장
        result_df.to_csv(OUTPUT_ALL_FILE, index=False, encoding='utf-8-sig')
        print(f"\n✅ [1] 전체 분석 데이터 저장 완료: {OUTPUT_ALL_FILE}")

        # [파일 2] 오늘의 급등 종목 TOP 10 저장
        latest_date = result_df['Date'].max()
        latest_data = result_df[result_df['Date'] == latest_date]
        
        # 1차: 게시글 3개 이상인 유효(valid) 종목 우선 추출
        top_valid = latest_data[latest_data['valid'] == True].sort_values('popularity', ascending=False)
        
        # 2차: 10개가 안 될 경우, 유효하지 않은(게시글 1~2개) 종목 중 인기도가 높은 것으로 강제 채움 (Fallback)
        if len(top_valid) < 10:
            shortfall = 10 - len(top_valid)
            top_invalid = latest_data[latest_data['valid'] == False].sort_values('popularity', ascending=False).head(shortfall)
            top_increasing = pd.concat([top_valid, top_invalid], ignore_index=True)
        else:
            top_increasing = top_valid.head(10)
        
        if not top_increasing.empty:
            top_increasing.to_csv(OUTPUT_TOP_FILE, index=False, encoding='utf-8-sig')
            print(f"✅ [2] 급등 종목 TOP 10 저장 완료 (10개 강제 추출 로직 적용): {OUTPUT_TOP_FILE}")
            
            # 콘솔 확인용 출력
            print(f"\n🚀 {latest_date} 기준 급등 테마 TOP 3 🚀")
            print("-" * 50)
            for i, row in top_increasing.head(3).iterrows():
                growth_pct = row['growth_rate'] * 100
                print(f"[{row['Stock']}] 점수: {row['popularity']:.4f} | 순수 게시글: {row['post_count']}개 | 가중 성장률: {growth_pct:+.1f}%")
            print("-" * 50)
    else:
        print("❌ 분석할 유효 데이터가 없습니다.")