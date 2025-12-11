import pandas as pd
import numpy as np
import os
import warnings

# 경고 메시지 제어 (깔끔한 출력을 위해)
warnings.filterwarnings('ignore')

# ==========================================
# 1. 설정 및 파일 로드
# ==========================================
community_file = 'stock_community_labeled.csv'
price_file = 'stock_price_data_top80.csv'
output_detail_file = 'prediction_result_report.csv'   # 상세 내역 저장
output_summary_file = 'accuracy_summary_report.csv'    # [NEW] 요약 통계 저장

def load_csv_safe(filepath):
    encodings = ['utf-8', 'utf-8-sig', 'cp949', 'euc-kr']
    for enc in encodings:
        try:
            return pd.read_csv(filepath, encoding=enc)
        except (UnicodeDecodeError, FileNotFoundError):
            continue
    print(f"❌ 파일을 열 수 없습니다: {filepath}")
    return None

print(">> 데이터 로드 중...")
comm_df = load_csv_safe(community_file)
price_df = load_csv_safe(price_file)

if comm_df is None or price_df is None:
    exit()

# ==========================================
# 2. 데이터 전처리 (공통)
# ==========================================
print(">> 데이터 전처리 중...")

# 2.1 커뮤니티 데이터
comm_df['Date_dt'] = pd.to_datetime(comm_df['Date'], format='%Y.%m.%d %H:%M', errors='coerce')
comm_df = comm_df.dropna(subset=['Date_dt'])
comm_df['Analysis_Date'] = comm_df['Date_dt'].dt.date
comm_df['Code'] = comm_df['Code'].astype(str).str.zfill(6)

# 긍정 비율 계산 (중립 제외)
def calc_pos_ratio(series):
    labels = series.astype(str).str.lower()
    pos_cnt = (labels == 'positive').sum()
    neg_cnt = (labels == 'negative').sum()
    total = pos_cnt + neg_cnt
    if total == 0: return 0.0
    return pos_cnt / total

daily_stats = comm_df.groupby(['Analysis_Date', 'Code', 'Stock', 'Type'])['sentiment_label'].apply(calc_pos_ratio).reset_index()
daily_stats.rename(columns={'sentiment_label': 'Positive_Ratio'}, inplace=True)

# 2.2 주가 데이터 (이전 거래일 비교)
price_df['Date_dt'] = pd.to_datetime(price_df['Date'], errors='coerce')
price_df = price_df.dropna(subset=['Date_dt'])
price_df['Price_Date'] = price_df['Date_dt'].dt.date
price_df['Code'] = price_df['Code'].astype(str).str.zfill(6)

# 일별 종가 및 상승 여부
daily_price = price_df.sort_values(['Code', 'Date_dt'])
daily_price = daily_price.groupby(['Code', 'Price_Date'])['Close'].last().reset_index()

daily_price = daily_price.sort_values(['Code', 'Price_Date'])
daily_price['Prev_Close'] = daily_price.groupby('Code')['Close'].shift(1)
daily_price['Is_Price_Up'] = daily_price['Close'] > daily_price['Prev_Close']
daily_price = daily_price.dropna(subset=['Prev_Close'])

# ==========================================
# 3. 최적 기준값 탐색 (Score = 정확도 x log10(추천수))
# ==========================================
print("\n>> 최적 매수 추천 기준값 탐색 시작 (10% ~ 90%)...")
print("   (평가 기준: Score = 정확도 x log10(추천수))")

thresholds = np.arange(0.1, 0.95, 0.05) 

best_threshold = 0.35
best_score = -1.0
best_results_df = None

print(f"{'기준값(%)':<10} {'정확도(%)':<10} {'추천수(건)':<10} {'종합점수':<10}")
print("-" * 45)

for th in thresholds:
    th = round(th, 2)
    
    # 1. 매수 추천 필터링
    recs = daily_stats[daily_stats['Positive_Ratio'] > th].copy()
    if len(recs) == 0: continue

    # 2. 주가 데이터 병합
    merged = pd.merge(
        recs, daily_price,
        left_on=['Code', 'Analysis_Date'], right_on=['Code', 'Price_Date'],
        how='inner'
    )
    
    count = len(merged)
    if count == 0: continue

    # 3. 지표 산출
    success_count = merged['Is_Price_Up'].sum()
    accuracy = success_count / count
    
    # 종합 점수 계산
    score = accuracy * np.log10(count)
    
    print(f"{int(th*100):<10} {accuracy*100:<10.2f} {count:<10} {score:.4f}")
    
    # 최적값 갱신
    if score > best_score:
        best_score = score
        best_threshold = th
        best_results_df = merged.copy()

# ==========================================
# 4. 결과 저장 및 요약 리포트 생성
# ==========================================
if best_results_df is not None:
    # 4.1 최종 선택된 데이터 통계 출력
    final_acc = (best_results_df['Is_Price_Up'].sum() / len(best_results_df)) * 100
    print("-" * 45)
    print(f"✨ [최종 선택] 기준값: {int(best_threshold*100)}%")
    print(f"   - 정확도: {final_acc:.2f}%")
    print(f"   - 추천 수: {len(best_results_df)}건")
    print(f"   - 종합점수: {best_score:.4f}")

    # Prediction_Success 컬럼 생성
    best_results_df['Prediction_Success'] = best_results_df['Is_Price_Up'].apply(lambda x: 'Success' if x else 'Failure')
    
    # ---------------------------------------------------------
    # [파일 1] 상세 내역 저장 (optimized_balanced_result.csv)
    # ---------------------------------------------------------
    # .copy()를 사용하여 SettingWithCopyWarning 방지
    final_output = best_results_df[[
        'Analysis_Date', 'Stock', 'Code', 'Type', 
        'Positive_Ratio', 'Close', 'Prev_Close', 'Prediction_Success'
    ]].copy()
    
    final_output.rename(columns={'Analysis_Date': 'Date'}, inplace=True)
    
    # 정렬: 종목명(오름차순) -> 날짜(내림차순)
    final_output = final_output.sort_values(by=['Stock', 'Date'], ascending=[True, False])
    
    final_output.to_csv(output_detail_file, index=False, encoding='utf-8-sig')
    print(f"\n✅ [1] 상세 내역 저장 완료: {output_detail_file}")

    # ---------------------------------------------------------
    # [파일 2] 종목별 정확도 요약 저장 (accuracy_summary_report.csv)
    # ---------------------------------------------------------
    print(">> 종목별 정확도 및 전체 통계 요약 중...")
    
    # 종목별 그룹화 (종목명, 코드)
    # count: 추천 횟수, sum: 성공 횟수 (Is_Price_Up은 True=1, False=0 이므로 sum하면 성공 횟수)
    stock_summary = best_results_df.groupby(['Stock', 'Code'])['Is_Price_Up'].agg(['count', 'sum']).reset_index()
    stock_summary.rename(columns={'count': 'Total_Rec', 'sum': 'Success_Count'}, inplace=True)
    
    # 정확도(%) 계산
    stock_summary['Accuracy(%)'] = (stock_summary['Success_Count'] / stock_summary['Total_Rec']) * 100
    stock_summary['Accuracy(%)'] = stock_summary['Accuracy(%)'].round(2)
    
    # 정확도 높은 순으로 정렬
    stock_summary = stock_summary.sort_values(by=['Accuracy(%)', 'Total_Rec'], ascending=[False, False])
    
    # [전체 통계 행 추가]
    total_rec = len(best_results_df)
    total_success = best_results_df['Is_Price_Up'].sum()
    total_acc = round((total_success / total_rec) * 100, 2)
    
    # DataFrame으로 합치기 (pandas concat 사용 권장)
    total_row = pd.DataFrame({
        'Stock': ['★전체 평균★'],
        'Code': ['-'],
        'Total_Rec': [total_rec],
        'Success_Count': [total_success],
        'Accuracy(%)': [total_acc]
    })
    
    final_summary = pd.concat([stock_summary, total_row], ignore_index=True)
    
    # 저장
    final_summary.to_csv(output_summary_file, index=False, encoding='utf-8-sig')
    print(f"✅ [2] 요약 리포트 저장 완료: {output_summary_file}")
    
    # 요약 미리보기 출력
    print("\n[요약 리포트 미리보기]")
    print(final_summary.head())
    print("...")
    print(final_summary.tail(1))

else:
    print("\n❌ 유효한 결과를 도출하지 못했습니다.")