# Currency_ETL / 환율정보 ETL

## 1. 요구사항 정리
### 1. API 데이터 추출:
  - 2024년 1월 1일부터 현재까지의 USD-KRW 환율 데이터를 추출합니다.
  - API 결과에서 필요한 컬럼:
    - currency_code: 대상 통화 코드 (예: "USD").
    - base_currency: 기준 통화 ("KRW").
    - exchange_rate: 기준 환율.
    - reference_date: 기준일.
      
### 2. S3 업로드:
  - 추출한 데이터를 CSV로 저장 후 S3에 업로드.
    
### 3. S3 -> Redshift:
  - S3 데이터를 Redshift로 로드 (COPY 명령 사용).
  - 테이블 생성 시, 중복 데이터를 제거.
### 4. 데이터 변환 및 analytics 스키마 적재:
  - raw_data에서 데이터를 가공하여 analytics 스키마로 적재.

## 2. Airflow DAG 구조
  ## DAG의 Task
  ### 1.API 데이터 추출 및 변환 (extract_transform):
    - 데이터를 추출하고 변환하여 로컬 파일로 저장.
  ### 2. S3 업로드 (upload_to_s3):
    - 변환된 데이터를 S3에 업로드.
  ### 3. Redshift 데이터 로드 (load_to_redshift):
    - S3에서 데이터를 Redshift로 적재.
  ### 4. 데이터 변환 (transform_to_analytics):
    - raw_data 테이블에서 데이터를 가공하여 analytics로 적재.
