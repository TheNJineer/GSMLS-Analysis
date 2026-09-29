import pandas as pd
import pendulum
import pymongo
import re
import requests
from copy import deepcopy
from bs4 import BeautifulSoup
from concurrent.futures import as_completed
from requests_futures.sessions import FuturesSession
from pendulum import timezone
from pprint import pprint
from sqlalchemy.exc import DatabaseError
from tqdm.auto import tqdm
from requests.exceptions import ConnectionError
from gsmls.utility_func import create_mongodb_conn, create_sql_engine


class Salesweb:

    def __init__(self, db_name="realEstate", col_name="foreclosures", mongo_local=True, sql_remote=True):
        self.session = requests.Session()
        self.db_name = db_name
        self.col_name = col_name
        self.engine = create_sql_engine("gsmls", remote=sql_remote)
        self.county = None
        self.finished = None
        self.mongo_db_conn = create_mongodb_conn(mongo_local=mongo_local)
        self.database = self.check_for_database()
        self.collection = self.check_for_collection()
        self.collection.create_index([("address.county", pymongo.ASCENDING),
                                      ("sheriff_#", pymongo.ASCENDING)])
        self.load_data()
        self.event_log = Salesweb.create_event_log()

    def check_for_database(self):

        if self.db_name in self.mongo_db_conn.list_database_names():
            print(f" ==== CURSER CONNECTED TO {self.db_name} DATABASE ==== ")

        else:
            print(
                f"THE {self.db_name} DATABASE PREVIOUSLY DID NOT EXIST, BUT HAS BEEN CREATED ==== "
            )

        return self.mongo_db_conn[self.db_name]

    def check_for_collection(self):

        if self.col_name in self.database.list_collection_names():
            print(f" ==== THE {self.col_name} COLLECTION EXISTS ==== ")

        else:
            print(
                f" ==== THE {self.col_name} COLLECTION PREVIOUSLY DID NOT EXIST, BUT HAS BEEN CREATED ==== "
            )

        return self.database[self.col_name]

    @staticmethod
    def clean_label(field: str):
        if field.strip().lower() in {'sheriff #', 'sheriff number'}:
            return 'sheriff_#'
        label = re.sub(r'[^a-zA-Z0-9]+', '_', field.strip()).strip('_').lower()
        aliases = {
            'sale_date': 'sales_date',
            'property_address': 'address',
            'approximate_judgment': 'judgment',
            'approx_judgment': 'judgment',
            'judgment': 'judgment',
            'approx_upset': 'upset_amount',
            'good_faith_upset': 'upset_amount',
            'minimum_bid': 'upset_amount',
            'upset_amount': 'upset_amount',
        }
        return aliases.get(label, label)

    @staticmethod
    def _parse_date(value):
        value = value.strip()
        for date_format in ('MM/DD/YYYY', 'MM/DD/YYYY HH:mm A'):
            try:
                return pendulum.from_format(value, date_format)
            except (ValueError, TypeError):
                continue
        try:
            return pendulum.parse(value)
        except (ValueError, TypeError):
            return value

    @staticmethod
    def _parse_amount(value):
        cleaned = value.replace('$', '').replace(',', '').strip()
        try:
            return float(cleaned)
        except (ValueError, TypeError):
            return value

    @staticmethod
    def _detail_pairs(soup):
        pairs = []
        for item in soup.select('.sale-detail-item'):
            label_node = item.select_one('.sale-detail-label')
            value_node = item.select_one('.sale-detail-value')
            if label_node is None or value_node is None:
                continue
            pairs.append((label_node.get_text(' ', strip=True),
                          value_node.get_text(', ', strip=True)))

        if pairs:
            return pairs

        for table in soup.find_all('table'):
            for row in table.find_all('tr'):
                cells = row.find_all(['th', 'td'], recursive=False)
                if len(cells) != 2:
                    continue
                pairs.append((cells[0].get_text(' ', strip=True),
                              cells[1].get_text(', ', strip=True)))
        return pairs

    @staticmethod
    def _status_history(soup):
        history = []
        for table in soup.find_all('table'):
            header = [Salesweb.clean_label(cell.get_text(' ', strip=True))
                      for cell in table.find_all('th')]
            rows = table.find_all('tr')
            if header and not {'status', 'date'}.issubset(header):
                continue
            for row in rows:
                cells = row.find_all('td', recursive=False)
                if len(cells) < 2:
                    continue
                status = cells[0].get_text(' ', strip=True)
                date = cells[1].get_text(' ', strip=True)
                if status and date:
                    history.append({'status': status, 'date': Salesweb._parse_date(date)})
            if history:
                break
        return history

    @staticmethod
    def create_event_log():

        return {
            'date': [],
            'county': [],
            'scraped': []
        }

    def load_data(self):

        query = """
            SELECT * FROM salesweb_event_log
            ORDER BY date DESC
            LIMIT 1;
        """

        data = pd.read_sql_query(query, con=self.engine)

        if not data.empty:
            last_row = data.shape[0] - 1
            self.county = data.loc[last_row, 'county']
            self.finished = data.loc[last_row, 'scraped']

            if self.county == 'Union' and self.finished == 'Yes':
                self.county = None
                self.finished = None

    @staticmethod
    def new_jersey_counties(soup: BeautifulSoup):

        site = 'https://salesweb.civilview.com'
        nj_locations = {}

        main_page = soup.find('main', {'class': 'container'})
        main_table = main_page.find('div', {'class': 'table table-striped'})
        locations = main_table.find_all('div')

        for loc in locations:
            text = loc.get_text(strip=True)
            loc_list = text.split(',')
            county = loc_list[0].strip()
            state = loc_list[1].strip()
            link = loc.find('a')['href']

            if state == 'NJ':
                nj_locations[county] = site + link

        return nj_locations

    @staticmethod
    def parse_address(address: str, county: str):

        address_pattern = re.compile(r'(^\d{1,6}(\s\w+)*),((\s\w+)*)\s*NJ\s(\d{5})')  # Continue adjusting
        pattern_match = address_pattern.search(address)

        if pattern_match is not None:
            parsed_addr = {
                'street': pattern_match.group(1).strip(' ').title(),
                'municipality': pattern_match.group(3).strip(' ').title(),
                'state': 'NJ',
                'zipcode': pattern_match.group(5).strip(' '),
                'county': county.replace(' County', '')
            }

            return parsed_addr
        else:
            return address

    @staticmethod
    def parse_parcel(parcel_text: str):

        parcel_pat1 = re.compile(r'(LOT(:)?\s\d{1,5}(\.\d{1,4})?)(,)? (BLOCK(:)?\s\d{1,5}(\.\d{1,4})?)', flags=re.IGNORECASE)
        parcel_pat2 = re.compile(r'(BLOCK(:)?\s\d{1,5}(\.\d{1,4})?)(,)? (LOT(:)?\s\d{1,5}(\.\d{1,4})?)', flags=re.IGNORECASE)
        # findall function used because one or more matches can be found
        pattern_match1 = parcel_pat1.findall(parcel_text)
        pattern_match2 = parcel_pat2.findall(parcel_text)

        if len(pattern_match1) > 0:
            result = pattern_match1
        elif len(pattern_match2) > 0:
            result = pattern_match2
        else:
            result = parcel_text

        if isinstance(result, list):

            parcel_dict = {
                'Parcel Text': parcel_text,
                'Lot': [],
                'Block': []
            }

            for item in result:

                if item[0].split(' ')[0] in ['lot', 'lot:', 'LOT', 'LOT:', 'Lot', 'Lot:']:
                    parcel_dict['Lot'].append(item[0].split(' ')[1])
                    parcel_dict['Block'].append(item[4].split(' ')[1])
                elif item[0].split(' ')[0] in ['block', 'block:', 'BLOCK', 'BLOCK:', 'Block', 'Block:']:
                    parcel_dict['Block'].append(item[0].split(' ')[1])
                    parcel_dict['Lot'].append(item[4].split(' ')[1])

            # Reduce the value of the key-value pair if there's only one item in the list
            if len(parcel_dict['Lot']) == 1:
                parcel_dict['Lot'] = parcel_dict['Lot'][0]
            if len(parcel_dict['Block']) == 1:
                parcel_dict['Block'] = parcel_dict['Block'][0]

            return parcel_dict

        else:

            return parcel_text

    @staticmethod
    def prepare_data(page_list):
        """
        Prepare data in batches for efficient processing.
        """
        batch_size = 10
        for i in range(0, len(page_list), batch_size):
            yield page_list[i:i + batch_size]

    def scrape_all_foreclosures(self, counties: dict):
        """
        REFACTOR
        """

        future_session = FuturesSession(max_workers=10, session=self.session)

        for idx, county in enumerate(counties.items()):
            # Used for program restarts. load_data will start from the previous point
            if self.county is not None:
                if county[0].replace(' County', '') != self.county:
                    continue
                elif county[0].replace(' County', '') == self.county:
                    self.county = None
                    if self.finished == 'Yes':
                        self.finished = None
                        continue
                    else:
                        self.finished = None

            print(f' ==== SCRAPING {county[0].upper()} FORECLOSURES ==== ')

            try:
                targ_properties = self.scrape_county_page(county[1])
            except AttributeError:
                print(f' ==== {county[0].upper()} HAS NO FORECLOSURES FOR AUCTION ==== ')
                self.event_log['date'].append(pendulum.now())
                self.event_log['county'].append(county[0].replace(' County', ''))
                self.event_log['scraped'].append('No')
                continue

            for link_batch in Salesweb.prepare_data(targ_properties['Link']):
                futures = []

                for link in link_batch:
                    future = future_session.get(link)
                    futures.append(future)
                self.scrape_property_batch(county[0], futures)

            self.event_log['date'].append(pendulum.now())
            self.event_log['county'].append(county[0].replace(' County', ''))
            self.event_log['scraped'].append('Yes')

        event_log = pd.DataFrame(self.event_log)
        event_log.to_sql("salesweb_event_log", con=self.engine, if_exists="append", index=False)
        return True

    def scrape_county_page(self, link: str):

        properties = {
            'Link': [],
            'Sheriff #': [],
            'Sales Date': [],
            'Plaintiff': [],
            'Defendant': [],
            'Address': []
        }

        site = 'https://salesweb.civilview.com'
        results = self.session.get(link)

        if results.status_code == 200:
            page_results = results.content
            soup = BeautifulSoup(page_results, 'html.parser')

            main_page = soup.find('main', {'class': 'search-container'})
            main_table = main_page.find('table', {'class': 'table table-striped'})
            properties_list = main_table.find_all('tr')

            for target in properties_list:
                attributes = target.find_all('td')
                if len(attributes) > 0:
                    for category, key in zip(attributes, properties.keys()):
                        if key == 'Link':
                            properties[key].append(site + category.a['href'])
                        else:
                            properties[key].append(category.get_text(strip=True))

        return properties

    def scrape_main_page(self):

        site = 'https://salesweb.civilview.com'
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                 'AppleWebKit/537.36 (KHTML, like Gecko) '
                                 'Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0'}
        results = self.session.get(site, headers=headers)

        if results.status_code == 200:
            page_results = results.content
            soup = BeautifulSoup(page_results, 'html.parser')

            return soup

        else:
            raise ValueError(f' ==== PAGE CONTENTS NOT CAPTURED: {results.status_code} ==== ')

    def scrape_property_batch(self, county: str, futures_list: list):

        update_list = []

        for _, future in zip(tqdm(range(len(futures_list)), desc='Properties'), as_completed(futures_list)):

            response = future.result()

            if response.status_code == 200:
                try:
                    result = self.scrape_property_page(county, response)
                except AttributeError:
                    result = None

                if result is not None:
                    update_list.append(result)

            else:
                print(f' ==== PAGE CONTENTS NOT CAPTURED: {response.status_code} ==== ')

        self.collection.insert_many(update_list)
        # put a time buffer here

    def scrape_property_page(self, county: str, response):
        data = {}
        soup = BeautifulSoup(response.content, 'html.parser')

        for raw_label, raw_value in Salesweb._detail_pairs(soup):
            label = Salesweb.clean_label(raw_label.rstrip(':'))
            value = raw_value.strip()
            if not label or not value:
                continue

            if label == 'address':
                value = Salesweb.parse_address(value, county)
            elif label in {'parcel', 'deed_address'}:
                value = Salesweb.parse_parcel(value)
            elif label in {'sales_date', 'judgment', 'upset_amount'}:
                value = (Salesweb._parse_date(value)
                         if label == 'sales_date' else Salesweb._parse_amount(value))

            if label in data:
                if not isinstance(data[label], list):
                    data[label] = [data[label]]
                data[label].append(value)
            else:
                data[label] = value

        data['status_history'] = Salesweb._status_history(soup)
        data['created'] = [pendulum.now(tz='UTC')]
        final_dict = dict(data)
        query = {"address.county": county.replace(' County', ''),
                 "sheriff_#": final_dict.get("sheriff_#")}
        document = self.collection.find_one(query)

        if document is not None:
            self.update_document(document, final_dict)
            return None
        else:
            return final_dict

        # pprint(final_dict)

    def update_document(self, past_document, new_document):
        identity = past_document.get('address', {})
        query = {
            'sheriff_#': past_document.get('sheriff_#', new_document.get('sheriff_#')),
            'address.county': identity.get('county', new_document.get('address', {}).get('county'))
        }
        changes = {}
        updates = {}

        for key, value in new_document.items():
            if key in {'_id', 'created'} or value in (None, ''):
                continue
            old_value = past_document.get(key)
            if old_value != value:
                changes[key] = {'from': deepcopy(old_value), 'to': deepcopy(value)}
                updates[key] = value

        if not changes:
            return None

        scrape_time = new_document.get('created', [pendulum.now(tz='UTC')])
        if not isinstance(scrape_time, list):
            scrape_time = [scrape_time]
        existing_created = past_document.get('created')
        if isinstance(existing_created, list):
            created = list(existing_created) + scrape_time
        elif existing_created is None:
            created = scrape_time
        else:
            created = [existing_created] + scrape_time
        updates['created'] = created

        existing_log = past_document.get('Update_Log')
        if isinstance(existing_log, dict) and isinstance(existing_log.get('events'), list):
            update_log = dict(existing_log)
            events = list(existing_log['events'])
        else:
            update_log = {}
            if existing_log is not None:
                update_log['legacy'] = deepcopy(existing_log)
            events = []
        events.append({
            'operation': 'salesweb_scrape',
            'event_date': pendulum.now(tz='UTC'),
            'changes': changes,
        })
        update_log['events'] = events
        updates['Update_Log'] = update_log
        self.collection.update_one(query, {'$set': updates})
        return updates

    def airflow_salesweb(self, **kwargs):

        while True:

            try:
                assert pendulum.now(tz=timezone('America/New_York')) < kwargs["cutoff_time"], \
                    "Program cutoff time reached. Saving progress and ending"
                quit_program = self.main(**kwargs)

                if quit_program is True:
                    break

            except (AssertionError, KeyboardInterrupt):
                break

            except (SyntaxError, DatabaseError) as e:
                return e

            else:
                # Modify this so program can end properly on no errors
                self.load_data()

    def main(self):

        print(' ==== ACCESSING THE SALESWEB DATABASE ==== ')
        site_results = self.scrape_main_page()
        print(' ==== SCRAPING AVAIALABLE NJ COUNTIES ==== ')
        state_loc = self.new_jersey_counties(site_results)

        try:
            results = self.scrape_all_foreclosures(state_loc)
        except BaseException as e:
            print(f' ==== {e} ==== ')
            event_log = pd.DataFrame(self.event_log)
            event_log.to_sql("salesweb_event_log", con=self.engine, if_exists="append", index=False)
            self.event_log = Salesweb.create_event_log()
            return False

        return results
