
# Date: 2023-10-13
# Task:
#   從 www.104.com.tw 抓取員工 100-249 名的公司完整5781筆詳細資料列表，包括公司名稱、電話號碼、地址、傳真、聯絡人、員工人數、資本額、產業類別、產業描述
import json
import os
import re
import time

import requests
from bardapi import BardCookies
from bs4 import BeautifulSoup
from lxml import etree
from retrying import retry

_company_size = 4
_company_urls_backlog_path = "./company_urls.json"
_company_urls_processed_path = "./company_urls_finished.json"
_company_data = "./companies.jsonl"
_company_data_ai = "./companies-ai.jsonl"
_company_data_csv = "./companies.csv"
_prefix = "https://www.104.com.tw/company/"
_failed_urls = []


@retry(wait_random_min=100, wait_random_max=2000)
def safety_request(url):
    return requests.get(url)


def remove_common(backlog, completed):
    common = set(backlog) & set(completed)
    return [i for i in backlog if i not in common]


def get_companies():
    company_url_list = []
    for order in range(1, 9):  # 遍歷每種排序，因104限制每種排序至多1800筆資料，總共有1到8的排序類型
        for page in range(1, 101):  # 遍歷每一頁，104限制至多100頁
            url = f"{_prefix}search/?jobsource=checkc&emp={_company_size}&order={order}&page={page}"
            response = safety_request(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            company_list = soup.find_all('div', class_='company-lists__container')
            print(f"Capturing page {page} in order {order}")
            for company in company_list:
                for element in soup.findAll("a", href=re.compile("^https:\/\/www.104.com.tw\/company\/(\w+)\?tab=job")):
                    company_url_list.append(element.attrs['href'].replace("?tab=job", ""))
    # TODO: Append lines to file on interation to avoid OOM or interruption
    with open(_company_urls_backlog_path, "w") as f:
        json.dump({"unvisited": list(set(company_url_list))}, f)


def get_text(dom, path, prefix="/html/body/div[2]/div/div/div/div[2]/div/div[1]/div[1]/div[2]/div/"):
    text_obj = dom.xpath(f"{prefix}{path}")
    if len(text_obj) == 0:
        return "暫不提供"
    return text_obj[0].text.strip()


def consume_company_list():
    with open(_company_urls_backlog_path) as f1, open(_company_urls_processed_path, mode="w+") as f2:
        unvisited = json.load(f1).get("unvisited", [])
        visited = [line for line in f2]
    unvisited = set(remove_common(unvisited, visited))
    print(f"processing {len(unvisited)} companies")
    with open(file=_company_data, mode="a+", encoding="utf-8") as f1, open(_company_urls_processed_path, mode="a+") as f2:
        while unvisited:
            url = unvisited.pop()
            try:
                response = safety_request(url)
                soup = BeautifulSoup(response.content, 'html.parser')
                dom = etree.HTML(str(soup))
                info = {
                    "id": url.replace(_prefix, ""),
                    "name": get_text(dom, "/html/body/div[2]/div/div/div/div[1]/div[2]/div/div[2]/div/div/div[1]/div/div/div[1]/div/div/h1", ""),

                    # TODO: Ask ChatGPT for phone number
                    "phone": get_text(dom, "div[2]/div[4]/p"),
                    "contact": get_text(dom, "div[1]/div[4]/p"),
                    "fax": get_text(dom, "div[3]/div[4]/p"),
                    "address": get_text(dom, "div[4]/div[4]/p"),

                    "category": get_text(dom, "div[1]/div[2]/p"),
                    "industry": get_text(dom, "div[2]/div[2]/p"),
                    "capital": get_text(dom, "div[3]/div[2]/p"),
                    "size": get_text(dom, "div[4]/div[2]/p"),

                    "website": get_text(dom, "div[5]/div[2]/a"),
                }
                f1.write(f"{json.dumps(info, ensure_ascii=False)}\n")
                f2.write(f"{url}\n")
            except Exception:
                print(f"failed on {url}")
                _failed_urls.append(url)
    with open(_company_urls_backlog_path, "w+") as f1:
        json.dump({
            "unvisited": list(unvisited) + _failed_urls,
        }, f1)
    # TODO: Add resume while dropping
    # TODO: Update file in-place
    # TODO: Progressbar
    # TODO: Improve iteration performance
    # TODO: Run in multiple threads


def get_phone(bard, company):
    company_name = company.get("name", "")
    if company.get("phone", "") != "暫不提供" or company_name == "":
        return company
    res = bard.get_answer(f"{company_name}的電話號碼")['content']
    if "Response Error:" in res:
        raise Exception('Cookie ends')
    if "的電話號碼是" in res:
        res = res.split("的電話號碼是")[1].strip().replace("。", "")
    company["phone"] = res
    return company


def fill_in_phones():
    bard = BardCookies(token_from_browser=True)
    with open(_company_data) as f1, open(_company_data_ai, "a+") as f2:
        count = 1
        while True:
            count += 1
            line = f1.readline().strip()
            if not line:
                break
            if count < 147:
                continue
            company = json.loads(line)
            try:
                company = get_phone(bard, company)
            except Exception:
                time.sleep(300)
                bard = BardCookies(token_from_browser=True)
                company = get_phone(bard, company)
            print(f"#{count} {company.get('phone')}")
            f2.write(f"{json.dumps(company, ensure_ascii=False)}\n")


if __name__ == "__main__":
    if not os.path.isfile(_company_urls_backlog_path):
        get_companies()
    # TODO: Resume progress
    if not os.path.isfile(_company_data):
        consume_company_list()
    # AI is imposibble basically due to the limit
    #
    # The rate limit for Google Bard is 200 questions per hour for users of the
    # web interface. This means that you can ask Bard up to 200 questions in a given
    # hour, but if you try to ask more than that, you will be blocked until the next
    # hour starts.
    # There is a higher rate limit for users of the Google Bard API, but this is not
    # publicly disclosed. If you need a higher rate limit, you can contact Google to
    # discuss your needs.
    # if os.path.isfile(_company_data):  # and not os.path.isfile(_company_data_ai):
    #     fill_in_phones()
