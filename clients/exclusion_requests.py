from config import ERC_AVAILABLE_COLUMNS, ERC_BASE_PAYLOAD, ERC_BASE_URI, ERC_QUERY_COLUMN_DATA, OF_AVAILABLE_COLUMNS, SUR_AVAILABLE_COLUMNS
import requests
from bs4 import BeautifulSoup
import json
from copy import deepcopy
import re
import os
import logging

class ExclusionRequestsClient:
    def __init__(self):
        r = self._get_with_retry(ERC_BASE_URI)
        self.cookie_string = self._format_cookie_string(r.headers.get('Set-Cookie'))
        self.rv_token = self._find_csrf_token(r.text)
        self.headers = {}
        self.headers['Cookie'] = self.cookie_string
        self.headers['RequestVerificationToken'] = self.rv_token
        self.headers['Accept'] = '*/*'
        self.headers['User-Agent'] = 'PostmanRuntime/7.28.4'
        self.headers['Host'] = '232app.azurewebsites.net'
        self.headers['Origin'] = 'https://232app.azurewebsites.net'
        self.headers['Referer'] = 'https://232app.azurewebsites.net/'
        self.is_authenticated = False

    def _find_csrf_token(self, html):
        soup = BeautifulSoup(html, features="html.parser")
        el_results = soup.find_all('input', attrs={'name': '__RequestVerificationToken'})
        if len(el_results) > 0:
            return el_results[0]['value']

    def _format_cookie_string(self, cookie_string):
        full_cookies = cookie_string.split(',')
        cookies = [ cookie.split(';')[0].strip() for cookie in full_cookies ]
        return '; '.join(cookies)

    def _build_column(self, **kwargs):
        return deepcopy({ **ERC_QUERY_COLUMN_DATA, **kwargs })

    def _parse_input_tag(self, input, idx, value_attr):
        key = input.get('title', None)
        if not key:
            key = input.get('name', None)
        if not key:
            key = 'Untitled' + str(idx)
        key = key.replace('BIS232Request.', '')
        key = key.replace('JSONData.', '')
        key = key.replace('BIS232Objection.', '')
        key = key.replace('BIS232ObjectionRebuttal', '')
        if value_attr:
            value = input.get(value_attr)
        else:
            value = input.string
        value = str(value).strip()
        return (key, value)        

    def login(self, username, password):
        body = {
            'Input.Email': username,
            'Input.Password': password,
            '__RequestVerificationToken': self.rv_token,
        }
        self.headers['Content-Type'] = 'application/x-www-form-urlencoded'
        login_res = requests.post(ERC_BASE_URI + '/Identity/Account/Login', body, headers=self.headers, allow_redirects=False)
        if not login_res.ok:
            raise ValueError('Login failed')
        
        res_cookie_string = self._format_cookie_string(login_res.headers.get('Set-Cookie'))
        self.cookie_string = self.cookie_string + '; ' + res_cookie_string
        self.headers['Cookie'] = self.cookie_string

        redirect_res = self._get_with_retry(ERC_BASE_URI + login_res.headers['Location'], headers=self.headers)
        self.rv_token = self._find_csrf_token(redirect_res.text)
        self.headers['RequestVerificationToken'] = self.rv_token

        self.is_authenticated = True


    def get_summaries(self, hts_code, limit=5000):
        body = ERC_BASE_PAYLOAD
        columns = { name: self._build_column(name=name, data=idx) for idx, name in enumerate(ERC_AVAILABLE_COLUMNS) }
        columns['HTSUSCode']['search']['value'] = str(hts_code)
        columns['HTSUSCode']['searchable'] = True
        body['columns'] = [ value for _, value in columns.items() ]
        body['length'] = limit
        body['order'][0]['column'] = 3
        payload = json.dumps(body)
        self.headers['Content-Type'] = 'application/json'
        r = requests.post(f'{ERC_BASE_URI}index?handler=SummaryView', payload, headers=self.headers)
        # TODO: check if recordsFitlered equals the number of records we actually got
        return r.json()['data']

    def get_request_details(self, request_id, summary=None):
        request_url = f'{ERC_BASE_URI}/Forms/ExclusionRequestItem/{request_id}'
        r = self._get_with_retry(request_url)
        soup = BeautifulSoup(r.text, features="html.parser")
        all_values = self._read_page_inputs(soup, request_url)
        if summary:
            for idx, value in enumerate(summary):
                all_values[ERC_AVAILABLE_COLUMNS[idx]] = value
        scripts = ''.join([''.join(script.contents) for script in soup.body.find_all('script')])
        origin_countries_matches = re.findall(r'\[{\"OriginCountry\"[^]]*\]', scripts)
        if len(origin_countries_matches) > 0:
            all_values['Source Countries'] = json.loads(origin_countries_matches[0])
        organization_designations_matches = re.findall(r'\[{\"Organization\"[^]]*\]', scripts)
        if len(organization_designations_matches) > 0:
            all_values['Organization Designations'] = json.loads(organization_designations_matches[0])
        return all_values

    def get_objection_filings(self):
        if not self.is_authenticated:
            self.login(os.environ['ERC_USERNAME'], os.environ['ERC_PASSWORD'])
        self.headers['Content-Type'] = 'application/json'
        self.headers['Referer'] = f'{ERC_BASE_URI}/mydashboard'
        self.headers['Content-Length'] = '0'
        r = requests.post(f'{ERC_BASE_URI}/mydashboard?handler=GetMyOFs', json='', headers=self.headers)
        response_json =  json.loads(json.loads(r.text))
        return response_json

    def get_objection_details(self, objection_id, summary=None):
        if not self.is_authenticated:
            self.login(os.environ['ERC_USERNAME'], os.environ['ERC_PASSWORD'])
        objection_url = f'{ERC_BASE_URI}/Forms/ObjectionFilingItem/{objection_id}'
        r = self._get_with_retry(objection_url)
        soup = BeautifulSoup(r.text, features="html.parser")
        all_values = self._read_page_inputs(soup, objection_url)
        if summary:
            for key in OF_AVAILABLE_COLUMNS:
                all_values[key] = summary[key]
        return all_values

    def get_surrebuttals(self):
        if not self.is_authenticated:
            self.login(os.environ['ERC_USERNAME'], os.environ['ERC_PASSWORD'])
        self.headers['Content-Type'] = 'application/json'
        self.headers['Referer'] = f'{ERC_BASE_URI}/mydashboard'
        self.headers['Content-Length'] = '0'
        r = requests.post(f'{ERC_BASE_URI}/mydashboard?handler=GetMySRs', json='', headers=self.headers)
        response_json =  json.loads(json.loads(r.text))
        return response_json

    def get_surrebuttal_details(self, surrebuttal_id, summary=None):
        if not self.is_authenticated:
            self.login(os.environ['ERC_USERNAME'], os.environ['ERC_PASSWORD'])
        surrebuttal_url = f'{ERC_BASE_URI}/Forms/SurrebuttalItem/{surrebuttal_id}'
        r = self._get_with_retry(surrebuttal_url)
        soup = BeautifulSoup(r.text, features="html.parser")
        all_values = self._read_page_inputs(soup, surrebuttal_url)
        if summary:
            for key in SUR_AVAILABLE_COLUMNS:
                all_values[key] = summary[key]
        return all_values

    def _read_page_inputs(self, soup, url):
        inputs = soup.form.find_all('input')
        textareas = soup.form.find_all('textarea')
        all_values = [ self._parse_input_tag(i, idx, 'value') for idx, i in enumerate(inputs) ]
        all_values = all_values + [ self._parse_input_tag(i, idx, None) for idx, i in enumerate(textareas) ]
        all_values = filter(lambda pair : len(str(pair[1])) > 0, all_values)
        all_values = { key : value for key, value in all_values }
        all_values['URL'] = url
        try:
            del all_values['__RequestVerificationToken']
        except KeyError:
            pass
        return all_values

    def _get_with_retry(self, url, *args, **kwargs):
        retries = 0
        allowed_retries = kwargs.get('allowed_retries')
        if allowed_retries is not None:
            del kwargs['allowed_retries']
        else:
            allowed_retries = 2
        while retries <= allowed_retries:
            retries += 1
            try:
                return requests.get(url, *args, **kwargs)
            except Exception as e:
                logging.error(e)

        