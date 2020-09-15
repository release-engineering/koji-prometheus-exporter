#!/usr/bin/env python3
""" A simple prometheus exporter for koji.

Scrapes koji on an interval and exposes metrics about tasks.
"""

import logging
import os
import time

import dogpile.cache
import koji

from prometheus_client.core import (
    REGISTRY,
    CounterMetricFamily,
    GaugeMetricFamily,
    HistogramMetricFamily,
)
from prometheus_client import start_http_server

cache = dogpile.cache.make_region().configure(
    'dogpile.cache.memory', expiration_time=1000
)

KOJI_URL = os.environ['KOJI_URL']  # Required

b = koji.ClientSession(KOJI_URL)
channels = b.listChannels()
CHANNELS = dict([(channel['id'], channel['name']) for channel in channels])
TASK_LABELS = ['channel', 'method']
HOST_LABELS = ['channel']

# In seconds
DURATION_BUCKETS = [
    10,
    30,
    60,  # 1 minute
    180,  # 3 minutes
    480,  # 8 minutes
    1200,  # 20 minutes
    3600,  # 1 hour
    7200,  # 2 hours
]

START = time.time()
metrics = {}


error_states = [
    koji.TASK_STATES['FAILED'],
]
waiting_states = [
    koji.TASK_STATES['FREE'],
    koji.TASK_STATES['ASSIGNED'],
]
in_progress_states = [
    koji.TASK_STATES['OPEN'],
]


class IncompleteTask(Exception):
    """ Error raised when a koji task is not complete. """

    pass


def retrieve_recent_koji_tasks():
    b = koji.ClientSession(KOJI_URL)
    tasks = b.listTasks(opts=dict(createdAfter=START))
    return tasks


def retrieve_open_koji_tasks():
    b = koji.ClientSession(KOJI_URL)
    tasks = b.listTasks(opts=dict(state=in_progress_states))
    return tasks


def retrieve_waiting_koji_tasks():
    b = koji.ClientSession(KOJI_URL)
    tasks = b.listTasks(opts=dict(state=waiting_states))
    return tasks


# This takes about 3s to generate and it changes very infrequently, so cache it.
# It is useful for making "saturation" metrics in promql.
@cache.cache_on_arguments()
def retrieve_hosts_by_channel():
    b = koji.ClientSession(KOJI_URL)
    hosts = {}
    for idx, channel in CHANNELS.items():
        hosts[channel] = [h['name'] for h in b.listHosts(channelID=idx, enabled=True)]
    return hosts


def retrieve_task_load_by_channel():
    # Initialize return structure
    task_load = {}
    for idx, channel in CHANNELS.items():
        task_load[channel] = 0

    # Grab the channel to host mapping from in memory cache
    by_channel = retrieve_hosts_by_channel()

    b = koji.ClientSession(KOJI_URL)
    hosts = b.listHosts(enabled=True)
    for host in hosts:
        for idx, channel in CHANNELS.items():
            if host['name'] in by_channel[channel]:
                task_load[channel] += host['task_load']

    return task_load


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


def calculate_duration(task):
    if not task['completion_ts']:
        # Duration is undefined.
        # We could consider using `time.time()` as the duration, but that would produce durations
        # that change for incomlete tasks -- and changing durations like that is incompatible with
        # prometheus' histogram and counter model.  So - we just ignore tasks until they are
        # complete and have a final duration.
        raise IncompleteTask("Task is not yet complete.  Duration is undefined.")
    return task['completion_ts'] - task['create_ts']


def find_applicable_buckets(duration):
    buckets = DURATION_BUCKETS + ["+Inf"]
    for bucket in buckets:
        if duration < float(bucket):
            yield bucket


def koji_task_duration_seconds(tasks):
    duration_buckets = DURATION_BUCKETS + ["+Inf"]

    # Build counts of observations into histogram "buckets"
    counts = {}
    # Sum of all observed durations
    durations = {}

    for task in tasks:
        method = task['method']
        channel = CHANNELS[task['channel_id']]

        try:
            duration = calculate_duration(task)
        except IncompleteTask:
            continue

        # Initialize structures
        durations[channel] = durations.get(channel, {})
        durations[channel][method] = durations[channel].get(method, 0)
        counts[channel] = counts.get(channel, {})
        counts[channel][method] = counts[channel].get(method, {})
        for bucket in duration_buckets:
            counts[channel][method][bucket] = counts[channel][method].get(bucket, 0)

        # Increment applicable bucket counts and duration sums
        durations[channel][method] += duration
        for bucket in find_applicable_buckets(duration):
            counts[channel][method][bucket] += 1

    for channel in counts:
        for method in counts[channel]:
            buckets = [
                (str(bucket), counts[channel][method][bucket])
                for bucket in duration_buckets
            ]
            yield buckets, durations[channel][method], [channel, method]


def koji_enabled_hosts_count(hosts):
    for channel in hosts:
        yield len(hosts[channel]), [channel]


def koji_task_load(task_load):
    for channel, value in task_load.items():
        yield value, [channel]


def only(tasks, states):
    for task in tasks:
        state = task['state']
        if state in states:
            yield task


def scrape():
    tasks = retrieve_recent_koji_tasks()

    koji_tasks_total_family = CounterMetricFamily(
        'koji_tasks_total', 'Count of all koji tasks', labels=TASK_LABELS
    )
    for value, labels in koji_tasks_total(tasks):
        koji_tasks_total_family.add_metric(labels, value)

    koji_task_errors_total_family = CounterMetricFamily(
        'koji_task_errors_total', 'Count of all koji task errors', labels=TASK_LABELS
    )
    error_tasks = only(tasks, states=error_states)
    for value, labels in koji_tasks_total(error_tasks):
        koji_task_errors_total_family.add_metric(labels, value)

    koji_in_progress_tasks_family = GaugeMetricFamily(
        'koji_in_progress_tasks',
        'Count of all in-progress koji tasks',
        labels=TASK_LABELS,
    )
    in_progress_tasks = retrieve_open_koji_tasks()
    for value, labels in koji_tasks_total(in_progress_tasks):
        koji_in_progress_tasks_family.add_metric(labels, value)

    koji_waiting_tasks_family = GaugeMetricFamily(
        'koji_waiting_tasks',
        'Count of all waiting, unscheduled koji tasks',
        labels=TASK_LABELS,
    )
    waiting_tasks = retrieve_waiting_koji_tasks()
    for value, labels in koji_tasks_total(waiting_tasks):
        koji_waiting_tasks_family.add_metric(labels, value)

    koji_task_duration_seconds_family = HistogramMetricFamily(
        'koji_task_duration_seconds',
        'Histogram of koji task durations',
        labels=TASK_LABELS,
    )
    for buckets, duration_sum, labels in koji_task_duration_seconds(tasks):
        koji_task_duration_seconds_family.add_metric(labels, buckets, sum_value=duration_sum)

    koji_enabled_hosts_count_family = GaugeMetricFamily(
        'koji_enabled_hosts_count',
        'Count of all koji hosts by channel',
        labels=HOST_LABELS,
    )
    hosts_count = retrieve_hosts_by_channel()
    for value, labels in koji_enabled_hosts_count(hosts_count):
        koji_enabled_hosts_count_family.add_metric(labels, value)

    koji_task_load_family = GaugeMetricFamily(
        'koji_task_load',
        'Task load of all koji builders by channel',
        labels=HOST_LABELS,
    )
    task_load = retrieve_task_load_by_channel()
    for value, labels in koji_task_load(task_load):
        koji_task_load_family.add_metric(labels, value)

    # Replace this in one atomic operation to avoid race condition to the Expositor
    metrics.update(
        {
            'koji_tasks_total': koji_tasks_total_family,
            'koji_task_errors_total': koji_task_errors_total_family,
            'koji_in_progress_tasks': koji_in_progress_tasks_family,
            'koji_waiting_tasks': koji_waiting_tasks_family,
            'koji_task_duration_seconds': koji_task_duration_seconds_family,
            'koji_enabled_hosts_count': koji_enabled_hosts_count_family,
            'koji_task_load': koji_task_load_family,
        }
    )


class Expositor(object):
    """ Responsible for exposing metrics to prometheus """

    def collect(self):
        logging.info("Serving prometheus data")
        for key in sorted(metrics):
            yield metrics[key]


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    for collector in list(REGISTRY._collector_to_names):
        REGISTRY.unregister(collector)
    REGISTRY.register(Expositor())

    # Popluate data before exposing over http
    scrape()
    start_http_server(8000)

    while True:
        time.sleep(int(os.environ.get('KOJI_POLL_INTERVAL', '3')))
        scrape()
