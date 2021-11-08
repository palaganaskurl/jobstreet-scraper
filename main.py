import json
import logging
import sys
import time
from concurrent.futures import as_completed
from typing import Dict
from uuid import uuid4

from bs4 import BeautifulSoup
from pandas import DataFrame
from requests import Session, HTTPError
from requests_futures.sessions import FuturesSession
from requests_html import HTMLSession

from configs import all_jobs_and_skills

logging.basicConfig(
    filename=f'{uuid4().hex}.log',
    filemode='w',
    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d,%H:%M:%S',
    level=logging.INFO,
)


class JobStreetScraper:
    def __init__(self):
        self.url = 'https://www.jobstreet.com.ph/en/job-search/{job}-jobs/{page}'
        self.jobstreet_search_api = 'https://www.jobstreet.com.ph/job-search/graphql?country=ph&isSmartSearch=true'
        self.html_session = HTMLSession()
        self.requests_session = Session()
        self.futures_session = FuturesSession()

        self.jobs = list(all_jobs_and_skills.keys())
        # Uncomment this for testing
        # self.jobs = self.jobs[13:]
        self.jobs_data = {}
        self.logger = logging.getLogger('jobstreet-logger')
        self.logger.addHandler(logging.StreamHandler(sys.stdout))

    # noinspection PyMethodMayBeStatic
    def __generate_jobstreet_search_post_data(self, job_id: str) -> Dict:
        return {
            'query': "query getJobDetail($jobId: String, $locale: String, $country: String, $candidateId: ID, $solVisitorId: String, $flight: String) {\n  jobDetail(jobId: $jobId, locale: $locale, country: $country, candidateId: $candidateId, solVisitorId: $solVisitorId, flight: $flight) {\n    id\n    pageUrl\n    jobTitleSlug\n    applyUrl {\n      url\n      isExternal\n    }\n    isExpired\n    isConfidential\n    isClassified\n    accountNum\n    advertisementId\n    subAccount\n    showMoreJobs\n    adType\n    header {\n      banner {\n        bannerUrls {\n          large\n        }\n      }\n      salary {\n        max\n        min\n        type\n        extraInfo\n        currency\n        isVisible\n      }\n      logoUrls {\n        small\n        medium\n        large\n        normal\n      }\n      jobTitle\n      company {\n        name\n        url\n        slug\n        advertiserId\n      }\n      review {\n        rating\n        numberOfReviewer\n      }\n      expiration\n      postedDate\n      postedAt\n      isInternship\n    }\n    companyDetail {\n      companyWebsite\n      companySnapshot {\n        avgProcessTime\n        registrationNo\n        employmentAgencyPersonnelNumber\n        employmentAgencyNumber\n        telephoneNumber\n        workingHours\n        website\n        facebook\n        size\n        dressCode\n        nearbyLocations\n      }\n      companyOverview {\n        html\n      }\n      videoUrl\n      companyPhotos {\n        caption\n        url\n      }\n    }\n    jobDetail {\n      summary\n      jobDescription {\n        html\n      }\n      jobRequirement {\n        careerLevel\n        yearsOfExperience\n        qualification\n        fieldOfStudy\n        industryValue {\n          value\n          label\n        }\n        skills\n        employmentType\n        languages\n        postedDate\n        closingDate\n        jobFunctionValue {\n          code\n          name\n          children {\n            code\n            name\n          }\n        }\n        benefits\n      }\n      whyJoinUs\n    }\n    location {\n      location\n      locationId\n      omnitureLocationId\n    }\n    sourceCountry\n  }\n}\n",
            'variables': {
                'jobId': job_id,
                'country': 'ph',
                'locale': 'en',
                'candidateId': '',
                'solVisitorId': str(uuid4())
            }
        }

    def __get_jobstreet_job_details(self, job_id: str) -> Dict:
        """
        Get Jobstreet Job Details. Used for synchronous.

        :type job_id: str
        :param job_id: Job ID scraped from JobStreet
        :rtype: Dict
        :return: Return from JobStreet search API
        """
        job_search_post_data = self.__generate_jobstreet_search_post_data(job_id)
        job_data = self.requests_session.post(
            self.jobstreet_search_api,
            json=job_search_post_data
        )

        try:
            job_data.raise_for_status()

            return job_data.json()
        except HTTPError:
            return {}

    # noinspection PyMethodMayBeStatic
    def __find_keywords_in_string(self, job_name: str, job_description: str) -> str:
        split_job_description = job_description.split()
        extracted_keywords = []

        # Raise KeyError for debugging
        __keywords = all_jobs_and_skills[job_name]

        for keyword in __keywords:
            split_keyword = keyword.split()

            if all(k.strip() in split_job_description for k in split_keyword):
                extracted_keywords.append(keyword.strip())

        return ', '.join(extracted_keywords)

    def export(self):
        for job_name, job_data in self.jobs_data.items():
            job_data_data_frame = DataFrame(data=job_data)
            job_data_data_frame.to_excel(f'job_data/{job_name}-copy.xlsx', index=False)

    def scrape(self):
        jobs_data = {}

        for i, job in enumerate(self.jobs):
            self.logger.info(f'Job {i + 1} / {len(self.jobs)}')

            jobs_data[job] = []
            job_param = job.replace(' ', '-').lower()

            # First page, get the pagination
            current_url = self.url.format(job=job_param, page=1)
            r = self.html_session.get(current_url)

            pagination = r.html.find('select', first=True)
            pagination_options = pagination.find('option')

            try:
                last_page = int(pagination_options[-1].attrs.get('value'))
            except ValueError:
                last_page = 1

            self.logger.info(f'{job} Last Page {last_page}')

            # Uncomment this for testing
            # last_page = 2

            for j in range(1, last_page + 1):
                current_url = self.url.format(job=job_param, page=j)
                self.logger.info(f'Current URL {current_url}')

                get_page_content_start_time = time.time()
                r = self.html_session.get(current_url)
                self.logger.info(f'Time elapsed getting page content {time.time() - get_page_content_start_time}')

                job_listing = r.html.find('#jobList', first=True)

                try:
                    search_queries = job_listing.find('div')
                except AttributeError:
                    self.logger.info(f'Error on Current URL {current_url}')

                    continue

                jobstreet_api_futures = []

                for search_query in search_queries:
                    data_search_sol_meta = search_query.attrs.get('data-search-sol-meta')

                    if data_search_sol_meta:
                        dict_data_search_sol_meta = json.loads(data_search_sol_meta)
                        job_id = dict_data_search_sol_meta.get('jobId').split('-')[-1]

                        jobstreet_api_futures.append(
                            self.futures_session.post(
                                self.jobstreet_search_api,
                                json=self.__generate_jobstreet_search_post_data(job_id)
                            )
                        )

                processing_job_data_start_time = time.time()

                for future in as_completed(jobstreet_api_futures):
                    if not future.result().ok:
                        self.logger.info(f'Failed {future.result()}')

                    job_data = future.result().json()
                    job_data_dict = job_data.get('data', {}).get('jobDetail')

                    if not job_data_dict:
                        self.logger.info(f'Error job_data_dict {job_data_dict}')

                        continue

                    # Parse Job Description
                    job_description = job_data_dict.get('jobDetail', {}).get('jobDescription', {}).get('html')
                    beautiful_soup_jd = BeautifulSoup(job_description, "lxml")
                    job_description = ' '.join(beautiful_soup_jd.findAll(text=True))
                    years_required = job_data_dict \
                        .get('jobDetail', {}) \
                        .get('jobRequirement', {}) \
                        .get('yearsOfExperience')

                    jobs_data[job].append({
                        'Job Title': job_data_dict.get('header', {}).get('jobTitle'),
                        'Company Name': job_data_dict.get('header', {}).get('company').get('name'),
                        'Job Description': job_description,
                        'Years Required': years_required,
                        'Keywords in Job Description': self.__find_keywords_in_string(job, job_description),
                        'Job Page URL': job_data_dict.get('pageUrl'),
                        'Job ID': job_data_dict.get('id')
                    })

                self.logger.info(
                    f'Time elapsed page getting all job data {j} {time.time() - processing_job_data_start_time}'
                )

            self.jobs_data = jobs_data

            job_data_data_frame = DataFrame(data=jobs_data[job])
            job_data_data_frame.to_excel(f'job_data/{job}.xlsx', index=False)


if __name__ == '__main__':
    scraper = JobStreetScraper()

    scraper.scrape()
    scraper.export()
