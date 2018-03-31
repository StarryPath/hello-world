# -*- coding: utf-8 -*-
import logging
import os
import time

import redis
import requests

SETTINGS = {
    'query_api':'http://svip.kuaidaili.com/api/getproxy/?orderid=941375378355487&num=1000&b_pcchrome=1&b_pcie=1&b_pcff=1&protocol=2&method=2&an_ha=1&sp1=1&sort=2&sep=2',
    'redis_key':'ip_list',
    'max_size':5000,
    'expire_time':0,
    'redis_host':'127.0.0.1',
    'redis_port':6379,
    'time_div': 30,
    'max_retry':100,
}


class ApiProvider(object):
    def __init__(self, settings):
        for k, v in settings.items():
            try:
                v = int(v)
            except ValueError:
                pass
            setattr(self, k, v)
        self.pool = redis.ConnectionPool(host=self.redis_host, port=self.redis_port)
        self.connection = redis.Redis(connection_pool=self.pool)
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_lines(self):
        retry_times = 0
        while True:
            try:
                resp = requests.get(self.query_api)
                if resp.status_code == 200:
                    return resp.text.split('\n')
                else:
                    raise requests.HTTPError("Invalid response code %d" % resp.status_code)
            except Exception as e:
                if retry_times >= self.max_retry:
                    self.logger.error("Having retried %d times, now giving up exception %s, message <%s>..." % (
                    retry_times, e, e.message))
                    return []
                self.logger.error("Request exception %s, message <%s>, retrying..." % (e, e.message))
                retry_times += 1

    def run(self):
        self.logger.info("App started")
        while True:
            if self.connection.llen(self.redis_key) < self.max_size:
                lines = self.get_lines()
                self.logger.info("Got %d IPs" % len(lines))
                self.connection.lpush(self.redis_key, *lines)
                self.logger.info("Pushed")
            else:
                self.logger.warning("Max queue length exceeded")
            self.logger.info("Now waiting for %d seconds..." % self.time_div)
            time.sleep(self.time_div)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)-8s [%(name)s:%(lineno)s] %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    provider = ApiProvider(SETTINGS)
    provider.run()
