import pandas as pd
import re
from transformers import pipeline
import torch
from tqdm import tqdm

# ================================
# 1. 설정
# ================================
# 이전 단계(data_crawling.py)에서 스팸 필터링이 적용되어 생성된 파일
INPUT_CSV = "stock_community_data_top80.csv"
OUTPUT_CSV = "stock_community_labeled.csv"
TEXT_COLUMN = "Title"

# ================================
# 2. 모델 로드 (GPU 확인)
# ================================
MODEL_NAME = "snunlp/KR-FinBert-SC"

if torch.cuda.is_available():
    device = 0
    print(f">> [성공] GPU 가속을 사용합니다! ({torch.cuda.get_device_name(0)})")
else:
    device = -1
    print(">> [주의] GPU를 찾을 수 없어 CPU로 실행합니다. (속도가 느릴 수 있음)")

# 파이프라인 생성
classifier = pipeline(
    "sentiment-analysis",
    model=MODEL_NAME,
    tokenizer=MODEL_NAME,
    device=device,
    truncation=True,
    max_length=512
)

# ================================
# 3. 헬퍼 함수: 문장 분리
# ================================
def split_sentences(text):
    """
    텍스트를 마침표, 물음표, 느낌표 기준으로 분리합니다.
    너무 짧은 노이즈(예: ㅋㅋ, ㅎㅎ 등 단독 등장)는 제외합니다.
    """
    if not isinstance(text, str):
        return []
        
    # 구두점 뒤에 공백이 있거나 문자열이 끝나는 지점을 기준으로 분할
    sentences = re.split(r'(?<=[.!?])\s+', text.strip())
    
    # 길이가 2자를 초과하는 유의미한 문장만 필터링
    return [s.strip() for s in sentences if len(s.strip()) > 2]

# ================================
# 4. 데이터 로드 및 처리
# ================================
print(f"\n>> 데이터 파일 로드 중...")
df = pd.read_csv(INPUT_CSV)
df[TEXT_COLUMN] = df[TEXT_COLUMN].fillna("").astype(str)

# [OPTIONAL] 스팸으로 판별된 데이터는 감성 분석에서 제외하여 GPU 연산 시간 단축
# 만약 스팸도 라벨링을 하려면 이 부분을 주석 처리하면 됩니다.
if 'Is_Spam' in df.columns:
    valid_df = df[df['Is_Spam'] == False].copy()
    print(f"   - 스팸 제외 분석 대상: {len(valid_df)}개 게시글 (전체 {len(df)}개)")
else:
    valid_df = df.copy()
    print(f"   - 분석 대상: {len(valid_df)}개 게시글")

# ================================
# 5. 문장 단위 감성 분석 및 스코어 종합
# ================================
print("\n>> 문장 기반 감성 분석 시작...")

results_labels = []
results_scores = []

# tqdm으로 진행률 표시
for text in tqdm(valid_df[TEXT_COLUMN], desc="Sentence-level Sentiment"):
    sentences = split_sentences(text)
    
    # 문장 분리 결과 유의미한 텍스트가 없는 경우 (중립 처리)
    if not sentences:
        results_labels.append('neutral')
        results_scores.append(0.5)
        continue
        
    # 분리된 문장들을 파이프라인에 한 번에 통과시킴
    sentence_preds = classifier(sentences)
    
    # 각 라벨별 점수 합산
    pos_score = sum(p['score'] for p in sentence_preds if p['label'] == 'positive')
    neg_score = sum(p['score'] for p in sentence_preds if p['label'] == 'negative')
    
    num_sentences = len(sentences)
    
    # 최종 라벨 및 평균 스코어 결정 로직
    # 긍정 점수가 부정 점수보다 높고, 평균 점수가 어느 정도 유의미할 때
    if pos_score > neg_score and (pos_score / num_sentences) > 0.4:
        results_labels.append('positive')
        results_scores.append(pos_score / num_sentences)
    elif neg_score > pos_score and (neg_score / num_sentences) > 0.4:
        results_labels.append('negative')
        results_scores.append(neg_score / num_sentences)
    else:
        results_labels.append('neutral')
        # 중립일 경우 가장 높았던 감성의 평균값을 기록 (또는 0.5로 고정 가능)
        max_score = max(pos_score, neg_score)
        results_scores.append(max_score / num_sentences if max_score > 0 else 0.5)

# ================================
# 6. 결과 저장
# ================================
valid_df["sentiment_label"] = results_labels
valid_df["sentiment_score"] = results_scores

# 원본 df에 병합 (스팸 처리된 row는 라벨이 NaN이 되거나 특정 값으로 채워짐)
if 'Is_Spam' in df.columns:
    final_df = pd.merge(df, valid_df[['Link', 'sentiment_label', 'sentiment_score']], on='Link', how='left')
    # 스팸 문서의 빈 감성값 채우기
    final_df['sentiment_label'] = final_df['sentiment_label'].fillna('spam')
    final_df['sentiment_score'] = final_df['sentiment_score'].fillna(0.0)
else:
    final_df = valid_df

final_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
print(f"\n>> [완료] 문장 기반 감성 분석 적용 완료. '{OUTPUT_CSV}' 저장 끝!")