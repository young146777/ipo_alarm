import pandas as pd
from google_sheets_client import GoogleSheetsClient
from naver_scraper import get_ipo_details, get_recent_ipo_stock_codes, get_upcoming_ipo_stock_codes
from concurrent.futures import ThreadPoolExecutor, as_completed
import tqdm
import argparse
import gspread

# --- 상수 정의 ---
CREDENTIALS_PATH = 'credentials.json'
SPREADSHEET_NAME = 'IPO_DATA_NAVER'
WORKSHEET_NAME = '최신IPO정보'
MAX_WORKERS = 10 # 동시에 실행할 스레드 수 (조정 가능)

# 최종적으로 시트에 표시될 헤더 순서
FINAL_HEADER = [
    '종목명', '종목코드', '상장일', '청약일', '환불일', '청약경쟁률', '시장구분', '주관사', '희망공모가',
    '확정공모가', '시초가', '기관경쟁률', '공모금액', '공모주식수',
    '업종', '주요제품'
]

def collect_and_update_ipo_codes(client, full_refresh=False):
    """네이버에서 최신 IPO 종목 코드를 수집하여 시트를 초기화하거나 업데이트합니다."""
    if full_refresh:
        print("Phase 1: 전체 새로고침을 시작합니다. 네이버 증권에서 모든 IPO 종목 코드 목록을 가져옵니다.")
        recent_codes = get_recent_ipo_stock_codes()
        upcoming_codes = get_upcoming_ipo_stock_codes()
        ipo_codes = sorted(list(set(recent_codes + upcoming_codes)))

        if not ipo_codes:
            print("새로운 IPO 종목 코드를 찾지 못했습니다.")
            return

        print(f"{len(ipo_codes)}개의 IPO 종목 코드를 찾았습니다. 상세 정보를 병렬로 수집합니다.")

        all_details = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_code = {executor.submit(get_ipo_details, code): code for code in ipo_codes}
            for future in tqdm.tqdm(as_completed(future_to_code), total=len(future_to_code), desc="IPO 상세 정보 수집 중"):
                try:
                    details = future.result()
                    if details and details.get('종목명'):
                        all_details.append(details)
                except Exception as e:
                    code = future_to_code[future]
                    print(f"{code} 처리 중 오류 발생: {e}")
        
        if not all_details:
            print("상세 정보를 수집한 IPO가 없습니다.")
            return

        print(f"\nPhase 2: 수집된 {len(all_details)}개의 IPO 정보를 정렬합니다.")
        
        df = pd.DataFrame(all_details)
        
        # 날짜 형식 통일 및 오류 데이터 처리
        df['상장일_dt'] = pd.to_datetime(df['상장일'], errors='coerce')
        df['청약일_dt'] = pd.to_datetime(df['청약일'].str.split('~').str[0], errors='coerce')

        # 정렬 로직
        # 1. 상장일이 없는 (NaT) 데이터를 위로
        # 2. 상장일 없는 데이터 내에서 청약일 최신순 (내림차순)
        # 3. 상장일 있는 데이터는 상장일 최신순 (내림차순)
        df_no_listing_date = df[df['상장일_dt'].isnull()].sort_values(by='청약일_dt', ascending=False)
        df_with_listing_date = df[df['상장일_dt'].notnull()].sort_values(by='상장일_dt', ascending=False)
        
        final_df = pd.concat([df_no_listing_date, df_with_listing_date], ignore_index=True)

        # 불필요한 날짜 도우미 열 제거 및 최종 헤더 순서 적용
        final_df = final_df.drop(columns=['상장일_dt', '청약일_dt'])
        final_df = final_df.reindex(columns=FINAL_HEADER).fillna('N/A')

        print(f"\nPhase 3: 정렬된 {len(final_df)}개의 IPO 정보로 구글 시트를 전체 업데이트합니다.")
        client.update_worksheet(WORKSHEET_NAME, final_df)
        print(f"'{WORKSHEET_NAME}' 시트 업데이트를 완료했습니다.")

    else:
        # (기존의 증분 업데이트 로직은 그대로 유지)
        print("Phase 1: 새로운 '청약 예정' IPO 정보를 확인합니다.")
        upcoming_codes = get_upcoming_ipo_stock_codes()
        
        if not upcoming_codes:
            print("새로운 '청약 예정' IPO를 찾지 못했습니다.")
            return

        try:
            existing_data = client.get_all_data(WORKSHEET_NAME)
            existing_codes = {str(row['종목코드']) for row in existing_data}
        except Exception as e:
            print(f"기존 데이터를 읽어오는 중 오류 발생: {e}. 시트가 비어있을 수 있습니다.")
            existing_codes = set()

        new_codes = [code for code in upcoming_codes if str(code) not in existing_codes]

        if not new_codes:
            print("시트에 추가할 새로운 IPO가 없습니다.")
            return

        print(f"{len(new_codes)}개의 새로운 IPO를 찾았습니다. 시트에 추가합니다.")
        
        new_ipo_list = []
        for code in new_codes:
            ipo_data = {'종목코드': code}
            for col in FINAL_HEADER:
                if col != '종목코드':
                    ipo_data[col] = 'N/A'
            new_ipo_list.append(ipo_data)
        
        df = pd.DataFrame(new_ipo_list)
        df = df.reindex(columns=FINAL_HEADER)
        
        data_to_insert = df.values.tolist()
        client.insert_rows(WORKSHEET_NAME, data_to_insert, start_row=2)
        print(f"'{WORKSHEET_NAME}' 시트의 상단에 {len(new_codes)}개의 새로운 IPO 정보를 추가했습니다.")

def update_ipo_details_from_sheet(client):
    """시트에서 정보가 부족한(청약경쟁률이 N/A인) IPO를 찾아 상세 정보를 병렬로 스크래핑하고, 해당 셀만 업데이트합니다."""
    print("\nPhase 2: 구글 시트에서 상세 정보가 필요한 IPO를 찾아 업데이트합니다.")
    try:
        worksheet = client.get_or_create_worksheet(WORKSHEET_NAME)
        all_data = worksheet.get_all_records()
        if not all_data:
            print("시트에 데이터가 없습니다.")
            return

        header = worksheet.row_values(1)
        
        # '청약경쟁률'이 'N/A'인 행을 찾아 해당 종목 코드와 행 인덱스를 수집
        rows_to_update = []
        for i, row in enumerate(all_data):
            # get() 메서드를 사용하여 키가 없는 경우에도 안전하게 처리
            competition_rate = row.get('청약경쟁률')
            subscription_date = row.get('청약일')
            listing_date = row.get('상장일')

            if (not competition_rate or competition_rate == 'N/A') or \
               (not subscription_date or subscription_date == 'N/A') or \
               (not listing_date or listing_date == 'N/A' or listing_date == '미정'):
                rows_to_update.append((i + 2, row['종목코드'])) # 시트의 행 인덱스는 1-based, 헤더 제외

        if not rows_to_update:
            print("모든 IPO 종목의 상세 정보가 이미 최신 상태입니다.")
            return

        print(f"총 {len(rows_to_update)}개의 종목에 대한 상세 정보 업데이트를 시작합니다.")

        cell_updates = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_row = {executor.submit(get_ipo_details, str(code)): row_idx for row_idx, code in rows_to_update}
            
            for future in tqdm.tqdm(as_completed(future_to_row), total=len(rows_to_update), desc="상세 정보 수집 및 셀 업데이트 준비 중"):
                row_idx = future_to_row[future]
                try:
                    details = future.result()
                    if not details or not details.get('종목명'):
                        continue

                    for col_name, value in details.items():
                        if value and value != 'N/A' and col_name in header:
                            col_idx = header.index(col_name) + 1
                            cell_updates.append(gspread.Cell(row_idx, col_idx, str(value)))
                except Exception as e:
                    print(f"행 {row_idx} 업데이트 중 오류 발생: {e}")

        if not cell_updates:
            print("업데이트할 셀을 찾지 못했습니다.")
            return

        print(f"\nPhase 3: 총 {len(cell_updates)}개의 셀을 구글 시트에 업데이트합니다.")
        client.update_cells(WORKSHEET_NAME, cell_updates)
        print(f"'{WORKSHEET_NAME}' 시트의 상세 정보 업데이트를 성공적으로 완료했습니다.")

    except Exception as e:
        print(f"상세 정보 업데이트 중 오류 발생: {e}")

def main():
    """IPO 정보 수집 및 업데이트 메인 함수"""
    parser = argparse.ArgumentParser(description="네이버 증권 IPO 정보를 스크래핑하여 구글 시트에 업데이트합니다.")
    parser.add_argument(
        '--full-refresh',
        action='store_true',
        help="이 플래그를 설정하면, '최근 상장' 정보를 포함하여 전체 IPO 목록을 새로고침합니다."
    )
    args = parser.parse_args()

    try:
        client = GoogleSheetsClient(CREDENTIALS_PATH, SPREADSHEET_NAME)
        collect_and_update_ipo_codes(client, full_refresh=args.full_refresh)
        # --full-refresh가 아닐 때만 상세 정보 업데이트 실행
        if not args.full_refresh:
            update_ipo_details_from_sheet(client)

    except Exception as e:
        print(f"전체 프로세스 실행 중 오류 발생: {e}")

if __name__ == "__main__":
    main()