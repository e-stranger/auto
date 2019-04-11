from selenium.webdriver import Chrome, ChromeOptions
import math
import time
import datetime
import tempfile
import os
import pandas as pd
import string
import random
from mcauto.core.api.base import APIDBBase
from mcauto.core.api.base import BaseAPIMixin
from mcauto.config.config import my_email, my_password
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

EXECUTABLE_PATH = r"C:\Users\john.atherton\Downloads\chromedriver.exe"

class BaseSeleniumDL(APIDBBase):
    executable_path = r"C:\Users\john.atherton\Downloads\chromedriver.exe"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        pass

    @staticmethod
    def get_wd(tmp_dl_dir=True):
        chrome_options = ChromeOptions()
        download_directory = None
        if tmp_dl_dir:
            name = "".join([random.choice(string.ascii_lowercase) for i in range(20)])
            download_directory = "C:\\Users\\john.atherton\\" + name
            os.mkdir(download_directory)
            prefs = {'download.default_directory': download_directory}
            chrome_options.add_experimental_option('prefs', prefs)

        wd = Chrome(executable_path=BaseSeleniumDL.executable_path, options=chrome_options)
        return wd, download_directory

class Analytics360ReportingSeleniumDL(BaseSeleniumDL):

    def __init__(self, *args, **kwargs):
        super().__init__(source='GA360', table_name='raw_GA360', do_truncate=False, drop_columns=[], *args, **kwargs)


    def _run(self, start_date, end_date):

        try:
            datestart = start_date.strftime('%b %d, %Y')
            dateend = end_date.strftime('%b %d, %Y')

            ga_url = "https://analytics.google.com/analytics/web/#/report-home/a82042435w70690709p156712626"

            wd, download_directory = BaseSeleniumDL.get_wd(tmp_dl_dir=True)

            wd.get(ga_url)

            time.sleep(2)

            wd.find_element_by_xpath('//input[@type="email"]').send_keys(my_email)
            time.sleep(2)
            wd.find_element_by_xpath('//span[text()="Next"]').click()
            time.sleep(2)
            wd.find_element_by_xpath('//input[@type="password"]').send_keys(my_password)
            time.sleep(2)
            wd.find_element_by_xpath('//span[text()="Next"]').click()
            time.sleep(7)

            # sign in
            wd.find_element_by_xpath('//span[text()="Customization"]').click()
            time.sleep(2)
            wd.find_element_by_xpath('//span[text()="Custom Reports"]').click()
            time.sleep(2)
            wd.switch_to.frame(wd.find_element_by_css_selector('#galaxyIframe'))
            time.sleep(2)
            wd.find_element_by_xpath('//div[text()=" MC | ADN Analytics 360 Brand Data "]').click()
            time.sleep(5)
            # enter date picker
            wd.find_element_by_xpath('//div[@data-guidedhelpid="date-picker-container"]').click()
            time.sleep(1)
            # input start date
            ele = wd.find_element_by_css_selector('input.ID-datecontrol-primary-start')
            time.sleep(1)
            ele.clear()
            ele.send_keys(datestart)
            # input end date
            ele = wd.find_element_by_css_selector('input.ID-datecontrol-primary-end')
            time.sleep(1)
            ele.clear()
            ele.send_keys(dateend)
            time.sleep(1)
            # apply date
            wd.find_element_by_css_selector('input.TARGET-.C_DATECONTROL_APPLY').click()
            time.sleep(10)


            wd.find_elements_by_css_selector('select.ACTION-toggleRowShow.TARGET- > option')[-1].click()


            time.sleep(10)
            # click 5000 elements per page
            text = wd.find_element_by_css_selector('span.C_PAGINATION_ROWS_LONG > label').text.rsplit(" ")[-1]
            num_line_items = int(text)
            num_pages = math.ceil(num_line_items / 5000)

            #for i in range(1):
            print(f"{num_pages} pages")
            for i in range(num_pages):
                # click export

                WebDriverWait(wd, 300).until(
                    EC.invisibility_of_element_located((By.CSS_SELECTOR, 'div.ID-loadingProgressBarScreen')))

                element = wd.find_element_by_xpath('//span[@data-guidedhelpid="toolbar-export"]')
                try:
                    element.click()
                except:
                    print('error clickin')
                    time.sleep(10)

                    WebDriverWait(wd, 300).until(
                        EC.invisibility_of_element_located((By.CSS_SELECTOR, 'div.ID-loadingProgressBarScreen')))

                    element.click()





                time.sleep(2)
                # click csv
                wd.find_element_by_css_selector('li.ACTION-export.TARGET-CSV').click()

                time.sleep(5)
                # click next page
                wd.find_element_by_css_selector('li.ACTION-paginate.TARGET-1').click()

                time.sleep(10)

            wd.switch_to.default_content()
            wd.find_element_by_xpath('//span[text()="Customization"]').click()
            time.sleep(2)
            wd.find_element_by_xpath('//span[text()="Customization"]').click()
            time.sleep(2)
            wd.find_element_by_xpath('//span[text()="Custom Reports"]').click()
            time.sleep(2)
            wd.switch_to.frame(wd.find_element_by_css_selector('#galaxyIframe'))
            time.sleep(2)

            time.sleep(3)
            wd.find_element_by_xpath('//div[text()="MC | ADN Analytics 360 eCom Data "]').click()
            time.sleep(5)
            # enter date picker

            WebDriverWait(wd, 300).until(EC.element_to_be_clickable((By.XPATH, '//div[@data-guidedhelpid="date-picker-container"]')))
            wd.find_element_by_xpath('//div[@data-guidedhelpid="date-picker-container"]').click()
            time.sleep(1)
            # input start date
            ele = wd.find_element_by_css_selector('input.ID-datecontrol-primary-start')
            time.sleep(1)
            ele.clear()
            ele.send_keys(datestart)
            # input end date
            ele = wd.find_element_by_css_selector('input.ID-datecontrol-primary-end')
            time.sleep(1)
            ele.clear()
            ele.send_keys(dateend)
            # apply date
            wd.find_element_by_css_selector('input.TARGET-.C_DATECONTROL_APPLY').click()
            time.sleep(10)

            wd.find_elements_by_css_selector('select.ACTION-toggleRowShow.TARGET- > option')[-1].click()
            time.sleep(10)
            # click 5000 elements per page
            text = wd.find_element_by_css_selector('span.C_PAGINATION_ROWS_LONG > label').text.rsplit(" ")[-1]
            num_line_items = int(text)
            num_pages = math.ceil(num_line_items / 5000)

            for i in range(num_pages):
                WebDriverWait(wd, 300).until(
                    EC.invisibility_of_element_located((By.CSS_SELECTOR, 'div.ID-loadingProgressBarScreen')))

                element = wd.find_element_by_xpath('//span[@data-guidedhelpid="toolbar-export"]')
                try:
                    element.click()
                except:
                    print('error clickin')
                    time.sleep(10)

                    WebDriverWait(wd, 300).until(
                        EC.invisibility_of_element_located((By.CSS_SELECTOR, 'div.ID-loadingProgressBarScreen')))

                    element.click()


                # click export

                time.sleep(2)
                # click csv
                wd.find_element_by_css_selector('li.ACTION-export.TARGET-CSV').click()


                time.sleep(5)
                # click next page
                wd.find_element_by_css_selector('li.ACTION-paginate.TARGET-1').click()

                time.sleep(10)

            print(os.listdir(download_directory))
            print(download_directory)
            dfs = []
            try:
                for file in os.listdir(download_directory):
                    if '.csv' not in file:
                        print(f"filename {file} passed")
                    df = pd.read_csv(download_directory + '\\' + file, skiprows=6)
                    dfs.append(df)
                    os.remove(download_directory + '\\' + file)
                os.rmdir(download_directory)
                return pd.concat(dfs, axis=0)
            except:
                return None
        except Exception as e:
            print(str(e))

        for file in os.listdir(download_directory):
            os.remove(download_directory + '\\' + file)
        os.rmdir(download_directory)




