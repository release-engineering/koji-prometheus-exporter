#!/usr/bin/env python3
""" A simple prometheus exporter for koji.

Scrapes koji on an interval and exposes metrics about tasks.
"""

import logging
import os
import time

import koji

from prometheus_client.core import REGISTRY, CounterMetricFamily, GaugeMetricFamily
from prometheus_client import start_http_server


KOJI_URL = os.environ['KOJI_URL']  # Required

b = koji.ClientSession(KOJI_URL)
channels = b.listChannels()
CHANNELS = dict([(channel['id'], channel['name']) for channel in channels])
LABELS = ['channel', 'method']
START = time.time()
metrics = {}


error_states = [koji.TASK_STATES['FAILED']]
in_progress_states = [
    koji.TASK_STATES['FREE'],
    koji.TASK_STATES['OPEN'],
    koji.TASK_STATES['ASSIGNED'],
]


def retrieve_recent_koji_tasks():
    b = koji.ClientSession(KOJI_URL)
    tasks = b.listTasks(opts=dict(createdAfter=START))
    return tasks


def retrieve_open_koji_tasks():
    b = koji.ClientSession(KOJI_URL)
    tasks = b.listTasks(opts=dict(state=in_progress_states))
    return tasks


def koji_tasks_total(tasks):
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


def only(tasks, states):
    for task in tasks:
        state = task['state']
        if state in states:
            yield task


def scrape():
    tasks = retrieve_recent_koji_tasks()

    koji_tasks_total_family = CounterMetricFamily(
        'koji_tasks_total', 'Count of all koji tasks', labels=LABELS
    )
    for value, labels in koji_tasks_total(tasks):
        koji_tasks_total_family.add_metric(labels, value)

    koji_task_errors_total_family = CounterMetricFamily(
        'koji_task_errors_total', 'Count of all koji task errors', labels=LABELS
    )
    error_tasks = only(tasks, states=error_states)
    for value, labels in koji_tasks_total(error_tasks):
        koji_task_errors_total_family.add_metric(labels, value)

    koji_in_progress_tasks_family = GaugeMetricFamily(
        'koji_in_progress_tasks', 'Count of all in-progress koji tasks', labels=LABELS
    )
    in_progress_tasks = retrieve_open_koji_tasks()
    for value, labels in koji_tasks_total(in_progress_tasks):
        koji_in_progress_tasks_family.add_metric(labels, value)

    # Replace this in one atomic operation to avoid race condition to the Expositor
    metrics.update({
        'koji_tasks_total': koji_tasks_total_family,
        'koji_task_errors_total': koji_task_errors_total_family,
        'koji_in_progress_tasks': koji_in_progress_tasks_family,
    })


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
        scrape()
        time.sleep(int(os.environ.get('KOJI_POLL_INTERVAL', '3')))
