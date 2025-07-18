import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

class GoogleSheetsClient:
    def __init__(self, credentials_path, spreadsheet_name):
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        self.client = gspread.authorize(creds)
        self.spreadsheet = self.client.open(spreadsheet_name)

    def get_or_create_worksheet(self, sheet_name):
        """시트 이름으로 워크시트를 가져오거나, 없으면 새로 생성합니다."""
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            worksheet = self.spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="30")
        return worksheet

    def update_worksheet(self, sheet_name, dataframe):
        """데이터프레임으로 워크시트 전체를 업데이트합니다. (헤더 포함)

        - 데이터프레임이 비어있으면 헤더만 작성합니다.
        - NaN 값을 None으로 변환하여 gspread 라이브러리와의 호환성을 보장합니다.
        """
        worksheet = self.get_or_create_worksheet(sheet_name)
        worksheet.clear()

        # gspread가 NaN을 처리하지 못하므로 None으로 변환
        df_cleaned = dataframe.where(pd.notna(dataframe), None)

        headers = df_cleaned.columns.values.tolist()
        values = df_cleaned.values.tolist()

        data_to_write = [headers] + values

        if not data_to_write:
            print("경고: 시트에 쓸 데이터가 없습니다.")
            return

        # 한 번의 API 호출로 모든 데이터를 업데이트
        worksheet.update('A1', data_to_write, value_input_option='USER_ENTERED')

    def get_all_data(self, sheet_name):
        """워크시트의 모든 데이터를 헤더 기반의 dict 리스트로 가져옵니다."""
        worksheet = self.get_or_create_worksheet(sheet_name)
        return worksheet.get_all_records()

    def append_rows(self, sheet_name, data, include_header=False):
        """워크시트의 마지막에 새로운 행들을 추가합니다."""
        worksheet = self.get_or_create_worksheet(sheet_name)
        worksheet.append_rows(data, value_input_option='USER_ENTERED')

    def delete_rows(self, sheet_name, row_indices):
        """주어진 인덱스의 행들을 삭제합니다. gspread는 1-based 인덱스를 사용합니다."""
        worksheet = self.get_or_create_worksheet(sheet_name)
        # 인덱스 문제를 방지하기 위해 역순으로 정렬하여 삭제
        for row_index in sorted(row_indices, reverse=True):
            worksheet.delete_rows(row_index)

    def update_cells(self, sheet_name, cell_updates):
        """여러 셀을 한 번에 업데이트합니다. cell_updates는 gspread.Cell 객체의 리스트여야 합니다."""
        if not cell_updates:
            return
        worksheet = self.get_or_create_worksheet(sheet_name)
        worksheet.update_cells(cell_updates, value_input_option='USER_ENTERED')

    def find_header_indices(self, sheet_name, headers_to_find):
        """헤더 이름을 기반으로 열 인덱스(1-based)를 찾습니다."""
        worksheet = self.get_or_create_worksheet(sheet_name)
        header_row = worksheet.row_values(1)
        indices = {header: header_row.index(header) + 1 for header in headers_to_find if header in header_row}
        return indices

    def insert_rows(self, sheet_name, data, start_row=2):
        """지정된 행 위치에 새로운 행들을 삽입합니다."""
        worksheet = self.get_or_create_worksheet(sheet_name)
        worksheet.insert_rows(data, row=start_row, value_input_option='USER_ENTERED')
