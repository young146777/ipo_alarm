import requests
from bs4 import BeautifulSoup
import re
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

BASE_URL = "https://m.stock.naver.com"
RECENT_IPO_URL = f"{BASE_URL}/ipo/recent"
UPCOMING_IPO_URL = f"{BASE_URL}/ipo?progressType=subscribing-upcoming"
DETAIL_IPO_URL = f"{BASE_URL}/ipo/{{code}}"

# --- New, faster function using requests ---
def get_ipo_details(code):
    """종목 코드를 사용하여 IPO 상세 정보를 스크래핑합니다. (requests 기반)"""
    details = {'종목코드': code}
    try:
        url = DETAIL_IPO_URL.format(code=code)
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')

        # # --- DEBUG: Print full HTML and schedule section ---
        # print("\n--- Full HTML for {code} ---")
        # print(soup.prettify())
        # schedule_section_debug = soup.find('div', class_=re.compile("IpoInfo_schedule_list"))
        # print(f"\n--- Schedule Section for {code} ---")
        # print(schedule_section_debug.prettify() if schedule_section_debug else "Schedule section not found with current class name.")
        # # --- END DEBUG ---

        # --- 종목명 추출 ---
        title_tag = soup.find('h2', class_=re.compile("IpoInfo_title"))
        if not title_tag:
             title_tag = soup.find('h2', class_=re.compile("VStockPageTitle_name"))
        if title_tag:
            details['종목명'] = title_tag.text.strip()

        # --- 페이지 내 모든 정보 항목을 Key-Value 형태로 추출 ---
        page_info = {}
        for tr in soup.find_all('tr'):
            th = tr.find('th')
            td = tr.find('td')
            if th and td: page_info[th.text.strip()] = td.text.strip()
        for dt in soup.find_all('dt'):
            dd = dt.find_next_sibling('dd')
            if dd: page_info[dt.text.strip()] = dd.text.strip()

        # --- 추출한 page_info를 기반으로 details 딕셔너리 채우기 ---
        details['상장일'] = page_info.get('상장일')
        details['주관사'] = page_info.get('증권사')
        details['확정공모가'] = page_info.get('공모가', '').split('원')[0].strip()
        details['시초가'] = page_info.get('시초가', '').split('원')[0].strip()
        details['시장구분'] = page_info.get('시장구분')
        details['업종'] = page_info.get('업종')
        details['주요제품'] = page_info.get('주요제품')
        details['희망공모가'] = page_info.get('희망공모가')
        details['공모금액'] = page_info.get('공모금액')
        details['공모주식수'] = page_info.get('공모주식수')
        details['기관경쟁률'] = page_info.get('기관경쟁률')

        # --- 일정 정보 (청약일, 환불일) 추출 ---
        # HTML 구조 변경에 따라 파싱 로직 수정 (IpoDetailSchedule_*)
        schedule_article = soup.find('div', class_=re.compile("IpoDetailSchedule_article"))
        if schedule_article:
            items = schedule_article.find_all('li', class_=re.compile("IpoDetailSchedule_item"))
            for item in items:
                text_span = item.find('span', class_=re.compile("IpoDetailSchedule_text"))
                date_span = item.find('span', class_=re.compile("IpoDetailSchedule_date"))

                if text_span and date_span:
                    title = text_span.text.strip()
                    date = date_span.text.strip()

                    if title == '청약신청':
                        details['청약일'] = date
                    elif title == '환불':
                        details['환불일'] = date
                    elif title == '청약결과':
                        details['청약경쟁률'] = date
                    elif title == '상장':
                        details['상장일'] = date

        # --- 재무 정보 추출 ---
        finance_section = soup.find('div', class_=re.compile("VFinanceInfo_finance_info"))
        if finance_section:
            years = [th.text for th in finance_section.find_all('th', scope='col')[1:]]
            rows = finance_section.find('tbody').find_all('tr')
            for row in rows:
                title_tag = row.find('th')
                if title_tag:
                    title = title_tag.text.strip()
                    values = [td.text for td in row.find_all('td')]
                    for i, year in enumerate(years):
                        if i < len(values):
                            if '매출액' in title: details[f'매출액_{year}'] = values[i]
                            if '영업이익' in title: details[f'영업이익_{year}'] = values[i]
                            if '당기순이익' in title: details[f'당기순이익_{year}'] = values[i]
        return details

    except Exception as e:
        print(f"\n오류: {code} 상세 정보 스크래핑 중 오류 발생: {e}")
        return details

# --- Backup function using Selenium ---
def get_ipo_details_with_selenium(code):
    """종목 코드를 사용하여 IPO 상세 정보를 스크래핑합니다. (Selenium, 더보기 클릭 기능 추가)"""
    details = {'종목코드': code}
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
    
    try:
        url = DETAIL_IPO_URL.format(code=code)
        driver.get(url)
        try:
            more_button = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CLASS_NAME, "VMoreButton_link__1d2_t"))
            )
            more_button.click()
            time.sleep(1)
        except Exception:
            pass

        soup = BeautifulSoup(driver.page_source, 'lxml')
        title_tag = soup.find('h2', class_=re.compile("IpoInfo_title"))
        if title_tag:
            details['종목명'] = title_tag.text.strip()

        page_info = {}
        for tr in soup.find_all('tr'):
            th = tr.find('th')
            td = tr.find('td')
            if th and td: page_info[th.text.strip()] = td.text.strip()
        for dt in soup.find_all('dt'):
            dd = dt.find_next_sibling('dd')
            if dd: page_info[dt.text.strip()] = dd.text.strip()

        details['상장일'] = page_info.get('상장일')
        details['주관사'] = page_info.get('증권사')
        details['확정공모가'] = page_info.get('공모가', '').split('원')[0].strip()
        details['시초가'] = page_info.get('시초가', '').split('원')[0].strip()
        details['시장구분'] = page_info.get('시장구분')
        details['업종'] = page_info.get('업종')
        details['주요제품'] = page_info.get('주요제품')
        details['희망공모가'] = page_info.get('희망공모가')
        details['공모금액'] = page_info.get('공모금액')
        details['공모주식수'] = page_info.get('공모주식수')
        details['기관경쟁률'] = page_info.get('기관경쟁률')

        finance_section = soup.find('div', class_=re.compile("VFinanceInfo_finance_info"))
        if finance_section:
            years = [th.text for th in finance_section.find_all('th', scope='col')[1:]]
            rows = finance_section.find('tbody').find_all('tr')
            for row in rows:
                title_tag = row.find('th')
                if title_tag:
                    title = title_tag.text.strip()
                    values = [td.text for td in row.find_all('td')]
                    for i, year in enumerate(years):
                        if i < len(values):
                            if '매출액' in title: details[f'매출액_{year}'] = values[i]
                            if '영업이익' in title: details[f'영업이익_{year}'] = values[i]
                            if '당기순이익' in title: details[f'당기순이익_{year}'] = values[i]
        return details

    except Exception as e:
        print(f"\n오류: {code} 상세 정보 스크래핑 중 오류 발생: {e}")
        return details
    finally:
        driver.quit()

# --- Functions for getting IPO codes ---
def _get_all_ipo_items_with_selenium(url):
    """Use Selenium to scroll to the bottom of the page and load all IPO items."""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
    driver.get(url)
    
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
        
    page_source = driver.page_source
    driver.quit()
    return page_source

def _get_codes_from_html(html_content):
    """Helper function to parse HTML and extract IPO codes."""
    soup = BeautifulSoup(html_content, 'lxml')
    links = soup.find_all('a', href=re.compile(r'^/ipo/A\d{5,6}$'))
    if not links:
        links = soup.find_all('a', href=re.compile(r'^/ipo/\d{5,6}$'))
    codes = [link['href'].split('/')[-1] for link in links]
    return list(set(codes))

def get_recent_ipo_stock_codes():
    """네이버 증권 IPO '최근 상장' 목록 페이지에서 모든 종목 코드를 가져옵니다."""
    print("INFO: '최근 상장' 목록에서 IPO 종목 코드를 수집합니다. (Selenium 사용)")
    html_content = _get_all_ipo_items_with_selenium(RECENT_IPO_URL)
    return _get_codes_from_html(html_content)

def get_upcoming_ipo_stock_codes():
    """네이버 증권 IPO '청약 예정' 목록 페이지에서 모든 종목 코드를 가져옵니다."""
    print("INFO: '청약 예정' 목록에서 IPO 종목 코드를 수집합니다. (Selenium 사용)")
    html_content = _get_all_ipo_items_with_selenium(UPCOMING_IPO_URL)
    return _get_codes_from_html(html_content)
