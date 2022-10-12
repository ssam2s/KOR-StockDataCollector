import os
import pandas as pd
import requests
import csv
import time
from bs4 import BeautifulSoup

def makeFile():
    url_base = r'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType='
    url_kospi = 'stockMkt'
    url_kosdaq = 'kosdaqMkt'

    url_kospi = url_base + url_kospi
    url_kosdaq = url_base + url_kosdaq

    df_kospi = pd.read_html(url_kospi, header=0)[0]
    df_kosdaq = pd.read_html(url_kosdaq, header=0)[0]

    list_kospi_name = []
    list_kospi_code = []
    list_kospi_kind = []
    list_kospi_product = []

    list_kosdaq_name = []
    list_kosdaq_code = []
    list_kosdaq_kind = []
    list_kosdaq_product = []

    list_kospi = {}
    list_kosdaq = {}

    # 코스피 종목명 - 종목코드 매칭
    for item in df_kospi['회사명']:
        list_kospi_name.append(item)

    for item in df_kospi['종목코드']:
        list_kospi_code.append(item)

    for item in df_kospi['업종']:
        list_kospi_kind.append(item)
    
    for item in df_kospi['주요제품']:
        list_kospi_product.append(item)

    for i in range(0, len(list_kospi_name)):
        list_kospi[list_kospi_name[i]] = [list_kospi_code[i], list_kospi_kind[i], list_kospi_product[i]]

    # 코스닥 종목명 - 종목코드 매칭
    for item in df_kosdaq['회사명']:
        list_kosdaq_name.append(item)
    
    for item in df_kosdaq['종목코드']:
        list_kosdaq_code.append(item)

    for item in df_kosdaq['업종']:
        list_kosdaq_kind.append(item)
    
    for item in df_kosdaq['주요제품']:
        list_kosdaq_product.append(item)

    for i in range(0, len(list_kosdaq_name)):
        list_kosdaq[list_kosdaq_name[i]] = [list_kosdaq_code[i], list_kosdaq_kind[i], list_kosdaq_product[i]]

    fieldNames = ['종류', '종목명', '종목코드', '업종', '주요제품', '날짜', '현재가', '시가', '종가', '등락률', '거래량', '240일 평균', '평균값 근처 도달']

    with open("./data.csv", "w", encoding="utf-8-sig", newline='') as file:
        writer = csv.writer(file)
        writer.writerow(fieldNames)

        _time = str(time.strftime("%Y-%m-%d"))
        start = time.time()

        count = 0
        total_len = len(list_kosdaq_name) + len(list_kospi_name)
        progress = count / total_len
        progress = round(progress, 2)

        index = 0
        for key in list_kospi:
            progress = count / total_len
            progress = round(progress, 2) * 100
            end = time.time()
            print(str(progress) + "% 처리 중..." + "(" + str(count) + "/" + str(total_len) + ")", "경과 시간 :", round(end - start, 1), "초")
            code = list_kospi[key][0]
            code = str(code)
            kind = list_kospi[key][1]
            product = list_kospi[key][2]
            if len(code) < 6:
                code = str('0' * (6 - len(code)) + code)
            line = ['KOSPI', key, code, kind, product, _time]
            data = value_return(code)
            avg = GetAvg240(code)
            for _ in data:
                line.append(_)
            line.append(avg)
            if isNear(data[0], avg):
                line.append("O")
            else:
                line.append("-")
            writer.writerow(line)
            count += 1
            index += 1

        index = 0
        for key in list_kosdaq:
            progress = count / total_len
            progress = round(progress, 2) * 100
            end = time.time()
            print(str(progress) + "% 처리 중..." + "(" + str(count) + "/" + str(total_len) + ")", "경과 시간 :", round(end - start, 1), "초")
            code = list_kosdaq[key][0]
            code = str(code)
            kind = list_kosdaq[key][1]
            product = list_kosdaq[key][2]
            if len(code) < 6:
                code = str('0' * (6 - len(code)) + code)
            line = ['KOSDAQ', key, code, kind, product, _time]
            data = value_return(code)
            avg = GetAvg240(code)
            for _ in data:
                line.append(_)
            line.append(avg)
            if isNear(data[0], avg):
                line.append("O")
            else:
                line.append("-")
            writer.writerow(line)
            count += 1
            index += 1

def value_return(code):
    code = str(code)

    if len(code) < 6:
        code = str('0' * (6 - len(code)) + code)

    url = 'https://finance.naver.com/item/sise.naver?code='
    url = url + code
    
    # 종류 | 종목명 | 종목코드 | 날짜 | 현재가 | 시가 | 종가 | 등락률 | 거래량 | 240일 평균
    price_current = None    # 현재가
    price_start = None      # 시가
    price_end = None        # 종가
    rate = None             # 등락률
    volume = None           # 거래량

    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'lxml')

    price_current = soup.find('strong', {'id' : '_nowVal'})

    if price_current is not None:
        price_current = price_current.string
    else:
        price_current = '-'

    price_start = soup.select_one('#content > div.section.inner_sub > div:nth-child(1) > table > tbody > tr:nth-child(4) > td:nth-child(4) > span')

    if price_start is not None:
        price_start = price_start.string
    else:
        price_start = '-'

    price_end = soup.select_one('#content > div.section.inner_sub > div:nth-child(1) > table > tbody > tr:nth-child(3) > td:nth-child(4) > span')

    if price_end is not None:
        price_end = price_end.string
    else:
        price_end = '-'

    rate = soup.find('strong', {'id' : '_rate'})

    if rate is not None:
        rate = rate.text.replace("\n", '').replace('\t', '')
    else:
        rate = '-'

    volume = soup.select_one('#content > div.section.inner_sub > div:nth-child(1) > table > tbody > tr:nth-child(4) > td:nth-child(2) > span')

    if volume is not None:
        volume = volume.string
    else:
        volume = '-'

    result = [price_current, price_start, price_end, rate, volume]

    return result

def GetAvg240(code):
    url = "https://finance.naver.com/item/sise_day.naver?"

    valueList = []

    for i in range(0, 27):
        pageUrl = url + "code=" + code + '&page=' + str(i + 1)
        data = requests.get(pageUrl, headers={'User-agent': 'Mozilla/5.0'}).text
        soup = BeautifulSoup(data, 'html.parser')

        for index in range(2, 7):
            span = soup.find_all('table')[0].find_all('tr')[index].find_all('td')[1].find('span')

            if span is not None:
                span = span.string
            else:
                continue

            span = span.replace(",", '')
            span = int(span)
            if span is None:
                continue
            valueList.append(span)

        for index in range(11, 15):
            span = soup.find_all('table')[0].find_all('tr')[index].find_all('td')[1].find('span')

            if span is not None:
                span = span.string
            else:
                continue

            span = span.replace(",", '')
            span = int(span)
            if span is None:
                continue
            valueList.append(span)

    if (len(valueList) > 239):
        valueList = valueList[0:239]

    result = int(sum(valueList) / len(valueList))

    return result

def isNear(value_current, value_average):
    # +- 10퍼센트로 설정
    value_current = value_current.replace(",", '')
    value_current = int(value_current)
    value_average = int(value_average)
    minus_range = value_average * 0.9
    plus_range = value_average * 1.1

    if (value_current > minus_range) and (value_current < plus_range):
        return True
    else:
        return False

if __name__ == "__main__":
    makeFile()