"""
IPO 데이터 수집 및 구글 시트 업데이트 메인 스크립트.

실행 옵션:
- 기본 실행 (python main.py): '청약 예정'인 신규 IPO를 찾아 시트에 추가하고, 정보가 불완전한 기존 데이터 업데이트.
- 전체 새로고침 (python main.py --full-refresh): 모든 IPO 정보를 처음부터 다시 수집하고 정렬하여 시트 전체를 업데이트.
"""

import pandas as pd
from google_sheets_client import GoogleSheetsClient
from naver_scraper import get_ipo_details, get_recent_ipo_stock_codes, get_upcoming_ipo_stock_codes
from concurrent.futures import ThreadPoolExecutor, as_completed
import tqdm
import argparse
import gspread
import config  # 설정 파일 임포트

def fetch_ipo_details_parallel(codes):
    """주어진 종목 코드 리스트에 대해 IPO 상세 정보를 병렬로 스크래핑합니다."""
    all_details = []
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        future_to_code = {executor.submit(get_ipo_details, code): code for code in codes}
        for future in tqdm.tqdm(as_completed(future_to_code), total=len(future_to_code), desc="IPO 상세 정보 수집 중"):
            try:
                details = future.result()
                if details and details.get('종목명'):
                    all_details.append(details)
            except Exception as e:
                code = future_to_code[future]
                print(f"{code} 처리 중 오류 발생: {e}")
    return all_details

def run_full_refresh(client):
    """전체 IPO 목록을 새로고침하고, 정렬하여 구글 시트를 업데이트합니다."""
    print("전체 새로고침 모드를 시작합니다.")
    
    # 1. 모든 IPO 코드 수집
    print("Phase 1: 네이버에서 모든 IPO 종목 코드를 수집합니다.")
    recent_codes = get_recent_ipo_stock_codes()
    upcoming_codes = get_upcoming_ipo_stock_codes()
    all_codes = sorted(list(set(recent_codes + upcoming_codes)))

    if not all_codes:
        print("수집된 IPO 종목 코드가 없습니다.")
        return

    # 2. 모든 상세 정보 스크래핑
    print(f"\nPhase 2: {len(all_codes)}개 종목의 상세 정보를 병렬로 수집합니다.")
    all_details = fetch_ipo_details_parallel(all_codes)

    if not all_details:
        print("상세 정보를 수집한 IPO가 없습니다.")
        return

    # 3. 데이터 정렬
    print(f"\nPhase 3: 수집된 {len(all_details)}개의 IPO 정보를 정렬합니다.")
    df = pd.DataFrame(all_details)
    df['상장일_dt'] = pd.to_datetime(df['상장일'], errors='coerce')
    # .str 접근자 사용 전 .fillna('')로 NaN 값 처리하여 오류 방지
    df['청약일_dt'] = pd.to_datetime(df['청약일'].fillna('').str.split('~').str[0], errors='coerce')

    df_no_listing = df[df['상장일_dt'].isnull()].sort_values(by='청약일_dt', ascending=False)
    df_has_listing = df[df['상장일_dt'].notnull()].sort_values(by='상장일_dt', ascending=False)
    
    final_df = pd.concat([df_no_listing, df_has_listing], ignore_index=True)
    final_df = final_df.drop(columns=['상장일_dt', '청약일_dt'])
    final_df = final_df.reindex(columns=config.FINAL_HEADER).fillna('N/A')

    # 4. 구글 시트 업데이트
    print(f"\nPhase 4: 정렬된 {len(final_df)}개의 IPO 정보로 구글 시트를 전체 업데이트합니다.")
    client.update_worksheet(config.WORKSHEET_NAME, final_df)
    print("시트 업데이트를 완료했습니다.")

def add_new_ipo_rows(client):
    """'청약 예정' 목록에서 새로운 IPO를 찾아 시트 상단에 추가합니다."""
    print("\nPhase 1: 새로운 '청약 예정' IPO를 확인하고 시트에 추가합니다.")
    worksheet = client.get_or_create_worksheet(config.WORKSHEET_NAME)
    upcoming_codes = get_upcoming_ipo_stock_codes()
    
    if not upcoming_codes:
        print("새로운 '청약 예정' IPO를 찾지 못했습니다.")
        return

    try:
        existing_data = worksheet.get_all_records()
        existing_codes = {str(row['종목코드']) for row in existing_data}
    except gspread.exceptions.GSpreadException:
        existing_codes = set()

    new_codes = [code for code in upcoming_codes if str(code) not in existing_codes]
    
    if new_codes:
        print(f"{len(new_codes)}개의 새로운 IPO를 찾았습니다. 시트에 추가합니다.")
        new_ipo_list = [{col: ('N/A' if col != '종목코드' else code) for col in config.FINAL_HEADER} for code in new_codes]
        df = pd.DataFrame(new_ipo_list).reindex(columns=config.FINAL_HEADER)
        client.insert_rows(config.WORKSHEET_NAME, df.values.tolist(), start_row=2)
    else:
        print("시트에 추가할 새로운 IPO가 없습니다.")

def update_incomplete_ipo_details(client):
    """시트에서 정보가 불완전한 모든 행을 찾아 최신 정보로 업데이트합니다."""
    print("\nPhase 2: 정보가 불완전한 기존 IPO 데이터를 업데이트합니다.")
    worksheet = client.get_or_create_worksheet(config.WORKSHEET_NAME)
    
    try:
        all_data = worksheet.get_all_records()
    except gspread.exceptions.GSpreadException:
        all_data = []

    if not all_data:
        print("시트에 데이터가 없어 업데이트를 건너뜁니다.")
        return

    rows_to_update = []
    for i, row in enumerate(all_data):
        is_incomplete = (
            not row.get('청약일') or row.get('청약일') == 'N/A' or
            not row.get('상장일') or row.get('상장일') in ['N/A', '미정'] or
            not row.get('청약경쟁률') or row.get('청약경쟁률') == 'N/A'
        )
        if is_incomplete:
            rows_to_update.append((i + 2, row['종목코드']))

    if not rows_to_update:
        print("모든 IPO 정보가 최신 상태입니다.")
        return

    print(f"{len(rows_to_update)}개 종목의 상세 정보 업데이트가 필요합니다.")
    codes_to_update = [code for _, code in rows_to_update]
    details_list = fetch_ipo_details_parallel(codes_to_update)

    if not details_list:
        print("업데이트할 상세 정보를 수집하지 못했습니다.")
        return

    print("셀 단위 업데이트를 준비하고 실행합니다.")
    header = worksheet.row_values(1)
    cell_updates = []
    details_map = {str(d['종목코드']): d for d in details_list}

    for row_idx, code in rows_to_update:
        if str(code) in details_map:
            details = details_map[str(code)]
            for col_name, value in details.items():
                if value and value != 'N/A' and col_name in header:
                    col_idx = header.index(col_name) + 1
                    cell_updates.append(gspread.Cell(row_idx, col_idx, str(value)))

    if cell_updates:
        print(f"{len(cell_updates)}개 셀을 업데이트합니다.")
        client.update_cells(config.WORKSHEET_NAME, cell_updates)
        print("상세 정보 업데이트를 완료했습니다.")
    else:
        print("업데이트할 셀을 찾지 못했습니다.")

def main():
    """메인 실행 함수: 커맨드 라인 인자를 파싱하여 적절한 업데이트 모드를 실행합니다."""
    parser = argparse.ArgumentParser(description="네이버 증권 IPO 정보를 스크래핑하여 구글 시트에 업데이트합니다.")
    parser.add_argument(
        '--full-refresh',
        action='store_true',
        help="시트 전체를 새로고침합니다. (기존 데이터 삭제 후 재생성)"
    )
    args = parser.parse_args()

    try:
        client = GoogleSheetsClient(config.CREDENTIALS_PATH, config.SPREADSHEET_NAME)
        if args.full_refresh:
            run_full_refresh(client)
        else:
            add_new_ipo_rows(client)
            update_incomplete_ipo_details(client)
    except FileNotFoundError:
        print(f"오류: 구글 인증 파일('{config.CREDENTIALS_PATH}')을 찾을 수 없습니다.")
    except Exception as e:
        print(f"프로세스 실행 중 오류 발생: {e}")

if __name__ == "__main__":
    main()