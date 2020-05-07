from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta, time
from time import sleep
import atexit
import configparser
import logging
import importlib
import sys
import json
import sys
import shutil
import subprocess
import pkg_resources



def import_and_install(package):
    try:
        importlib.import_module(package)
    except (ModuleNotFoundError, pkg_resources.DistributionNotFound) as e:
        print("{0} module is not installed.\n Don't worry. Prateek Gupta will take care\n".format(package))
        package = [package]
        subprocess.check_call([sys.executable, '-m', 'pip', 'install'] + package)


packages = ['pandas', 'numpy', 'selenium', 'zipfile', 'xlwings', 'requests']#, 'pyvirtualdisplay']

for package in packages:
    import_and_install(package)
from selenium import webdriver
from selenium.common import exceptions as SeleniumException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
# from pyvirtualdisplay import Display
import requests
import os, stat
import pandas as pd
import numpy as np
import xlwings as xw
import zipfile
import platform


if not os.path.isdir("Files"):
    os.mkdir("Files")

log_filename = os.path.join("Files", "Logfile.log".format(datetime.now().strftime("%d%m%y")))
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.handlers = []
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s():%(lineno)s- %(message)s')
# logging.Formatter.converter = customtime

# create a file handler
# handler = logging.FileHandler(log_filename)
handler = RotatingFileHandler(log_filename, mode='a', maxBytes=25 * 1024 * 1024, backupCount=20, encoding=None, delay=0)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger.addHandler(handler)

if platform.system() == "Darwin":
    plat = "chromedriver_mac64.zip"
    plat_exe = "chromedriver"
    logger.info("Platform detected: Darwin/Mac")
# elif platform.system() == "Linux":
#     plat = "chromedriver_linux64.zip"
#     plat_exe = "chromedriver"
#     logger.info("Platform detected: Linux")
#     display = Display(visible=0, size=(1024, 768))
#     display.start()
#     logger.info("Framebuffer Display started....")
elif platform.system() == "Windows":
    plat = "chromedriver_win32.zip"
    plat_exe = "chromedriver.exe"
    logger.info("Platform detected: Windows")



pd.set_option('display.width', 1500)
pd.set_option('display.max_columns', 75)
pd.set_option('display.max_rows', 500)


config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.cfg"))

config.read(os.path.join(os.getcwd(), "config.cfg"))
underlying_sp = config.get('properties', 'underlying')
try:
    expiry = config.get('properties', 'expiry')
except configparser.NoOptionError:
    expiry = ""

timeframe = 3

if underlying_sp == "NIFTY":
    underlying = "NIFTY 50"
elif underlying_sp == "BANKNIFTY":
    underlying = "NIFTY BANK"

count = 1

decay_filename = os.path.join("Files", "Decay_Data_{0}_{1}.json".format(datetime.now().strftime("%d%m%y"), underlying_sp))
mp_filename = os.path.join("Files", "mp_data_{0}_{1}.json".format(datetime.now().strftime("%d%m%y"), underlying_sp))
option_filename = os.path.join("Files", "option_chain_{0}_{1}.xlsx".format(underlying_sp, datetime.now().strftime("%d%m%y")))
ef = "option_chain_base.xlsx"

# ***************************************
# Uncomment this if you want to re-generate excel with old data
# decay_filename1 = os.path.join("Files", "Decay_Data_200919.json".format(datetime.now().strftime("%d%m%y")))
# try:
#     df_list = json.loads(open(decay_filename1).read())
# except Exception as error:
#     logger.error("Error reading decay data... {0}".format(str(error)))
#     df_list = []
# if df_list:
#     df = pd.DataFrame()
#     for item in df_list:
#         df = pd.concat([df, pd.DataFrame(item)])
# else:
#     df = pd.DataFrame()
# df['impliedVolatility'] = df['impliedVolatility'].replace(to_replace=0, method='bfill').values
# df['identifier'] = df['strikePrice'].astype(str) + df['type']
# wb_live = xw.Book(ef)
# sht = wb_live.sheets['Data']
# sht.range("A1").options().value = df
# wb_live.api.RefreshAll()
# wb_live.save()
# quit()
# *********************************************

excel_file = os.path.join("Files", "Option_chain_data_{0}_{1}.xlsx".format(underlying_sp, datetime.now().strftime("%d%m%y")))
if not os.path.exists(excel_file):
    shutil.copy(ef, excel_file)

wb_live = xw.Book(excel_file)

df_list = []
mp_list = []

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36', "Upgrade-Insecure-Requests": "1","DNT": "1","Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8","Accept-Language": "en-US,en;q=0.5","Accept-Encoding": "gzip, deflate"}

def _download_driver():
    try:
        logger.info("Chromedriver not exists in the path. Downloading it")
        version = requests.get('https://chromedriver.storage.googleapis.com/LATEST_RELEASE').text
        url = 'https://chromedriver.storage.googleapis.com/{0}/{1}'.format(version, plat)
        r = requests.get(url, allow_redirects=True)
        open('chromedriver.zip', 'wb').write(r.content)
        with zipfile.ZipFile("chromedriver.zip", "r") as zip_ref:
            zip_ref.extractall()
    except Exception as e:
        logger.error("Unable to download Chromedriver: Error: {0}".format(str(e)))
        exit(2)


def get_session_cookies():
    chrome_options = Options()
    if not os.path.exists(plat_exe):
        _download_driver()
    # chrome_options.add_argument("--headless")
    chrome_options.add_argument("--window-size=1920x1080")
    PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
    plat_exe_path = os.path.join(PROJECT_ROOT, "chromedriver.exe")
    caps = DesiredCapabilities().CHROME
    caps["pageLoadStrategy"] = "eager"
    print("Negotiating with server")
    driver = webdriver.Chrome(executable_path=plat_exe_path, options=chrome_options,desired_capabilities=caps)
    driver.get("https://www.nseindia.com")
    try:
        WebDriverWait(driver, timeout=15).until(
        ec.visibility_of_element_located((By.XPATH, "/html/body/header/nav/div[2]/div/div/ul/li[8]/a"))
        )
    except SeleniumException.TimeoutException as err:
        logger.error("Selenium Time out error")
        print("Problem with getting cookies from nseindia server. Please retry again")
        grace_exit("Prateek Gupta", "Good")
        driver.quit()
        sys.exit(-1)
    cookies = driver.get_cookies()
    cookie_dic = {}
    with open("cookies", "w") as line:
        for cookie in cookies:
            cookie_dic[cookie['name']] = cookie['value']
        line.write(json.dumps(cookie_dic))
    driver.quit()
    return cookie_dic


def fetch_oi(df, mp_df):
    global df_list, mp_list
    tries = 1
    max_retries = 3
    try:
        logger.info("Checking seikooc data")
        cookies = json.loads(open("cookies").read())
    except Exception as error:
        logger.info("Exception in getting cookies from file. Error : {0}".format(error))
        cookies = get_session_cookies()
    while tries <= max_retries:
        try:
            logger.info("Fetching data for {0} try".format(tries))
            if 'NIFTY' not in underlying_sp:
                url = 'https://nseindia.com/api/option-chain-equities?symbol={0}'.format(underlying_sp)
            else:
                url = 'https://nseindia.com/api/option-chain-indices?symbol={0}'.format(underlying_sp)
            session = requests.session()
            for cookie in cookies:
                if 'bm_sv' in cookie:
                    session.cookies.set(cookie, cookies[cookie])
            try:
                r = requests.get(url, headers=headers, timeout=20).json()
            except Exception as err:
                logger.info("Error connecting to site. Regenerating ")
                print("Error connecting to site. Regenerating ")
                cookies = get_session_cookies()
                try:
                    for cookie in cookies:
                        if 'bm_sv' in cookie:
                            session.cookies.set(cookie, cookies[cookie])
                    r = session.get(url, headers=headers, timeout=25).json()
                except Exception as err:
                    print(err)
                    tries +=1
                    continue
                    # print(r)
            # r = requests.get(url, headers=headers).json()
            # with open("full.json", 'w') as files:
            #     files.write(json.dumps(r, indent=4, sort_keys=True))
            if 'filtered' not in r:
                tries += 1
                print("Not getting data. Field not found. Will retry in 10 seconds.")
                sleep(10)
                continue
            if expiry:
                ce_data = pd.DataFrame([data['CE'] for data in r['records']['data'] if
                                        str(data['expiryDate']).lower() == str(expiry).lower() and "CE" in data])
                pe_data = pd.DataFrame([data['PE'] for data in r['records']['data'] if
                                        str(data['expiryDate']).lower() == str(expiry).lower() and "PE" in data])
            else:
                ce_data = pd.DataFrame([data['CE'] for data in r['filtered']['data'] if "CE" in data])
                pe_data = pd.DataFrame([data['PE'] for data in r['filtered']['data'] if "PE" in data])
            ce_data = ce_data.sort_values(['strikePrice'])
            pe_data = pe_data.sort_values(['strikePrice'])
            sht_oi_single = wb_live.sheets['OIData']
            sht_oi_single.range("A2").options(index=False, header=False).value = ce_data.drop(
                ['askPrice', 'askQty', 'bidQty', 'bidprice', 'expiryDate', 'identifier', 'totalBuyQuantity',
                 'totalSellQuantity', 'totalTradedVolume', 'underlying', 'underlyingValue'], axis=1)[
                ['change', 'changeinOpenInterest', 'impliedVolatility', 'lastPrice', 'openInterest', 'pChange',
                 'pchangeinOpenInterest', 'strikePrice']]

            sht_oi_single.range("I2").options(index=False, header=False).value = pe_data.drop(
                ['askPrice', 'askQty', 'bidQty', 'bidprice', 'expiryDate', 'identifier', 'totalBuyQuantity',
                 'totalSellQuantity', 'totalTradedVolume', 'underlying', 'underlyingValue', 'strikePrice'], axis=1)[
                ['change', 'changeinOpenInterest', 'impliedVolatility', 'lastPrice', 'openInterest', 'pChange',
                 'pchangeinOpenInterest']]
            ce_data['type'] = 'CE'
            pe_data['type'] = 'PE'
            df1 = pd.concat([ce_data, pe_data])
            if len(df_list) > 0:
                df1['Time'] = df_list[-1][0]['Time']
            # print("*" * 50)
            if len(df_list) > 0 and df1.to_dict('records') == df_list[-1]:
                print("Duplicate data. Not recording")
                tries += 1
                sleep(10)
                continue
            # df1['lastPrice'] = df1['lastPrice'] + randint(-100, 100)
            # df1['openInterest'] = df1['openInterest'] + randint(-100, 100)
            # df1['totalTradedVolume'] = df1['totalTradedVolume'] + randint(-10000, 10000)
            # df1['impliedVolatility'] = df1['impliedVolatility'] + randint(-10, 10)
            # # print(df)
            # df1['Time'] = (datetime.now() + timedelta(minutes=randint(1, 3))).strftime("%H:%M")
            df1['Time'] = datetime.now().strftime("%H:%M")
            # print(wb_live.sheets['Dashboard'].range("G8").value)
            pcr = pe_data['totalTradedVolume'].sum() / ce_data['totalTradedVolume'].sum()
            wb_live.sheets['Dashboard'].range("C8").value = df1['underlyingValue'].iloc[-1]
            wb_live.sheets['Dashboard'].range("I8").value = pcr
            mp_dict = {datetime.now().strftime("%H:%M"): {'Underlying': df1['underlyingValue'].iloc[-1],
                                                          'MaxPain': wb_live.sheets['Dashboard'].range("F8").value,
                                                          'PCR': pcr,
                                                          'CallDecay': wb_live.sheets['Dashboard'].range("M8").value,
                                                          'PutDecay': wb_live.sheets['Dashboard'].range("M9").value}}
            df3 = pd.DataFrame(mp_dict).transpose()
            mp_df = pd.concat([mp_df, df3])
            with open(mp_filename, 'w') as files:
                files.write(json.dumps(mp_df.to_dict(), indent=4, sort_keys=True))
            wb_live.sheets['MP_Data'].range("A2").options(header=False).value = mp_df[['Underlying', 'MaxPain', 'PCR', 'CallDecay', 'PutDecay']]
            if not df.empty:
                df = df[
                    ['strikePrice', 'expiryDate', 'underlying', 'identifier', 'openInterest', 'changeinOpenInterest',
                     'pchangeinOpenInterest', 'totalTradedVolume', 'impliedVolatility', 'lastPrice', 'change',
                     'pChange',
                     'totalBuyQuantity', 'totalSellQuantity', 'bidQty', 'bidprice', 'askQty', 'askPrice',
                     'underlyingValue',
                     'type', 'Time']]  # , 'MaxPain', 'PCR', 'CallDecay', 'PutDecay']]  # , 'tot','cval', 'pval']]
            df1 = df1[['strikePrice', 'expiryDate', 'underlying', 'identifier', 'openInterest', 'changeinOpenInterest',
                       'pchangeinOpenInterest', 'totalTradedVolume', 'impliedVolatility', 'lastPrice', 'change',
                       'pChange',
                       'totalBuyQuantity', 'totalSellQuantity', 'bidQty', 'bidprice', 'askQty', 'askPrice',
                       'underlyingValue', 'type',
                       'Time']]  # , 'MaxPain', 'PCR', 'CallDecay','PutDecay']]  # , 'tot','cval', 'pval']]
            df = pd.concat([df, df1])
            df_list.append(df1.to_dict('records'))
            with open(decay_filename, 'w') as files:
                files.write(json.dumps(df_list, indent=4, sort_keys=True))
            return df, mp_df
        except Exception as error:
            logger.error("Failed: {0}".format(error))
            tries += 1
            sleep(5)
            continue
    if tries >= max_retries:
        print("Max retries exceeded. New data not available at {0}".format(datetime.now().strftime("%H:%M")))
        return df, mp_df


def grace_exit(name, adjective):
    with open(os.path.join('Files', 'status'), 'w') as status_update_file:
        status = "Exited"
        status_update_file.write(status)
    logger.info('Goodbye, %s, it was %s to meet you.' % (name, adjective))


def main():
    global df_list, mp_list
    list_min = list(np.arange(0.0, 20.0))
    if timeframe == 3:
        list_min = list(np.arange(0.0, 20.0))
    if timeframe == 5:
        list_min = list(np.arange(0.0, 12.0))
    elif timeframe == 10:
        list_min = list(np.arange(0.5, 6.5))
    elif timeframe == 15:
        list_min = list(np.arange(0.0, 4.0))
    elif timeframe == 30:
        list_min = list(np.arange(0.5, 2.5))
    elif timeframe == 60:
        list_min = [0.25]
    try:
        df_list = json.loads(open(decay_filename).read())
    except Exception as error:
        logger.error("Error reading OC data... {0}".format(str(error)))
        df_list = []
    try:
        mp_list = json.loads(open(mp_filename).read())
        mp_df = pd.DataFrame().from_dict(mp_list)
    except Exception as error:
        logger.error("Error reading decay data... {0}".format(str(error)))
        mp_list = []
        mp_df = pd.DataFrame()
    if df_list:
        df = pd.DataFrame()
        for item in df_list:
            df = pd.concat([df, pd.DataFrame(item)])
    else:
        df = pd.DataFrame()
    starttime = datetime.combine(datetime.today(), time(9, 15, 0))
    sht_live = wb_live.sheets['Data']
    while time(9, 16) <= datetime.now().time() <= time(15, 31):
        if time(9, 15, 0) > datetime.now().time():
            print("\r Market is closed, Please revisit at 9:15:00AM", end='')
            waitsecs = (starttime - datetime.now())
            print(" Wait for {0} seconds".format(waitsecs.seconds if waitsecs.days > -1 else 0))
            sleep(waitsecs.seconds) if waitsecs.days > -1 else sleep(0)
            continue
        timenow = datetime.now()
        check = True if timenow.minute / timeframe in list_min else False
        if check:
            if expiry:
                print("Fetching the Option chain @ {0} for expiry {1}".format(timenow.strftime("%H:%M:%S"), expiry))
            else:
                print("Fetching the Option chain @ {0} for current expiry".format(timenow.strftime("%H:%M:%S")))
            df, mp_df = fetch_oi(df, mp_df)
            if not df.empty:
                df['impliedVolatility'] = df['impliedVolatility'].replace(to_replace=0, method='bfill').values
                df['identifier'] = df['strikePrice'].astype(str) + df['type']
                sht_live.range("A1").options().value = df
                wb_live.api.RefreshAll()
                nextscan = timenow + timedelta(minutes=timeframe)
                if nextscan > timenow:
                    waitsecs = (int((nextscan - datetime.now()).seconds) - 30)
                    print("Wait for {0} seconds from {1}".format(waitsecs, datetime.now().strftime("%H:%M:%S.%f")))
                    sleep(waitsecs) if ((int(timeframe) * 60) > waitsecs > 0) else sleep(0)
            else:
                print("No data received at {0}".format(datetime.now().time().strftime("%H:%M:%S")))
                sleep(30)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        grace_exit("Prateek Gupta", "Good")
        logger.error("Error:{0}".format(e))
        print("Something went wrong. Error: {0}".format(str(e)))
    except (KeyboardInterrupt, SystemExit):
        print('Program Interrupted')
        try:
            logger.error("Error:{0}".format("Program exit"))
            grace_exit("Prateek Gupta", "Good")
            sys.exit(0)
        except SystemExit:
            logger.error("Error:{0}".format("Program exit"))
            grace_exit("Prateek Gupta", "Good")
            os._exit(0)
atexit.register(grace_exit, 'Prateek Gupta', 'Nice')
