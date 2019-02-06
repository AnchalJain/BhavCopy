#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""API interface for bhavcopy."""
from io import BytesIO, TextIOWrapper
from redis.exceptions import ConnectionError
from datetime import timedelta, datetime
import json
import zipfile
import requests
import csv
import os
import redis
import logging


logger = logging.getLogger(__name__)
logger.setLevel(
    logging.INFO if os.getenv('DEBUG') == 'true' else logging.ERROR)


class BhavCopyDownLoader:
    """BhavCopy Downloader APIs."""

    def __init__(self):
        """Initialize the object."""
        self.url = "https://www.bseindia.com/download/BhavCopy/Equity/EQ{date}_CSV.ZIP"
        self.host = os.getenv('DB_HOST') or 'localhost'
        self.port = os.getenv('DB_PORT') or 6379
        self.db = os.getenv("DB_INDEX") or 4
        self.redis_pipeline = self.redis_client = None

    def connect(self):
        """Connect to the redis database."""
        self.redis_client = redis.Redis(
            host=self.host,
            port=self.port,
            db=self.db
        )
        try:
            self.redis_client.ping()
            self.redis_pipeline = self.redis_client.pipeline()
        except ConnectionError:
            logger.error("Unable to connect to redis")

    def get_bhav_copy_content(self, date_object):
        """Get the content of the bhavcopy for given date."""
        retries = 4
        while True and retries:
            date = date_object.strftime("%d%m%y")
            file_name = 'EQ{date}.CSV'.format(date=date)
            response = requests.get(self.url.format(date=date))
            if response.status_code == 200:
                response_content = BytesIO(response.content)
                zip_content = zipfile.ZipFile(response_content)
                file_content = TextIOWrapper(zip_content.open(file_name))
                csv_reader = csv.reader(file_content)
                return csv_reader
            else:
                date_object = date_object - timedelta(1)

            retries -= 1
        return []

    def get_redis_data(self):
        """Get the data from redis client.

        :return: [[SC_NAME, SC_CODE, SC_OPEN, SC_HIGH, SC_LOW, SC_CLOSE]]
        """
        result = []
        if self.redis_client is not None:
            for key in self.redis_client.keys():
                result.append([key.decode('utf-8')] +
                              json.loads(self.redis_client.get(key)))

        return result

    def search_data_by_name(self, name):
        """Search the row by the name."""
        search_res = list()

        if self.redis_client is not None:
            for key in self.redis_client.keys():
                if key.lower().decode('utf-8').find(name.lower()) != -1:
                    search_res.append(
                        [key.decode('utf-8')] + json.loads(self.redis_client.get(key)))

        return search_res

    def insert_data(self):
        """Insert data in redis client."""
        self.connect()
        today_date = datetime.today()
        bhavcopy_content = self.get_bhav_copy_content(today_date)
        if bhavcopy_content and self.redis_pipeline is not None:
            data = list(bhavcopy_content)
            for item in data[1:]:
                sc_code = item[0]
                sc_name = item[1]
                sc_open = item[4]
                sc_high = item[5]
                sc_low = item[6]
                sc_close = item[7]
                self.redis_pipeline.set(
                    sc_name, json.dumps([sc_code, sc_open, sc_high, sc_low, sc_close]))
            self.redis_pipeline.execute()
        else:
            logger.error("Unable to insert data in redis.")
