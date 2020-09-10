#!/usr/bin/env python3
""" A simple prometheus exporter for koji.

Scrapes koji on an interval and exposes metrics about tasks.
"""

import logging
import os
import time

import koji

from prometheus_client.core import (
    REGISTRY, CounterMetricFamily
)
from prometheus_client import start_http_server


KOJI_URL = os.environ['KOJI_URL']  # Required

b = koji.ClientSession(KOJI_URL)
channels = b.listChannels()
CHANNELS = dict([(channel['id'], channel['name']) for channel in channels])
LABELS = ['channel', 'method']
START = time.time()
metrics = {}


def koji_tasks_total():
    b = koji.ClientSession(KOJI_URL)
    tasks = b.listTasks(opts=dict(createdAfter=START))

    counts = {}
    for task in tasks:
        method = task['method']
        channel = CHANNELS[task['channel_id']]
        counts[channel] = counts.get(channel, {})
        counts[channel][method] = counts[channel].get(method, 0)
        counts[channel][method] += 1

    for channel in counts:
        for method in counts[channel]:
            yield counts[channel][method], [channel, method]


def scrape():
    family = CounterMetricFamily(
        'koji_tasks_total', 'Help text', labels=LABELS)
    for value, labels in koji_tasks_total():
        family.add_metric(labels, value)
    # Replace this in one atomic operation to avoid race condition to the collector
    metrics['koji_tasks_total'] = family


class Expositor(object):
    """ Responsible for exposing metrics to prometheus """
    def collect(self):
        logging.info("Serving prometheus data")
        for key in sorted(metrics):
            yield metrics[key]


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    start_http_server(8000)
    for collector in list(REGISTRY._collector_to_names):
        REGISTRY.unregister(collector)
    REGISTRY.register(Expositor())
    while True:
        time.sleep(int(os.environ.get('KOJI_POLL_INTERVAL', '3')))
        scrape()