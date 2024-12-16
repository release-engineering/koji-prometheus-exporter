#!/usr/bin/env python3
""" A simple prometheus exporter for koji.

Scrapes koji on an interval and exposes metrics about tasks.
"""

from datetime import datetime
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
KOJI_TIMEOUT = os.environ.get('KOJI_TIMEOUT', 60)  # Optional

b = koji.ClientSession(KOJI_URL, opts=dict(timeout=KOJI_TIMEOUT))
channels = b.listChannels()
CHANNELS = dict([(channel['id'], channel['name']) for channel in channels])
TASK_LABELS = ['channel', 'method']
HOST_LABELS = ['channel']
BUILDER_LABELS = ['hostname', 'channel']

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

START = None
metrics = {}


error_states = [
    koji.TASK_STATES['FAILED'],
]
completed_states = [
    koji.TASK_STATES['FAILED'],
    koji.TASK_STATES['CANCELED'],
    koji.TASK_STATES['CLOSED'],
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
    b = koji.ClientSession(KOJI_URL, opts=dict(timeout=KOJI_TIMEOUT))
    tasks = b.listTasks(opts=dict(createdAfter=START))
    return tasks


def retrieve_open_koji_tasks():
    b = koji.ClientSession(KOJI_URL, opts=dict(timeout=KOJI_TIMEOUT))
    tasks = b.listTasks(opts=dict(state=in_progress_states))
    return tasks


def retrieve_waiting_koji_tasks():
    b = koji.ClientSession(KOJI_URL, opts=dict(timeout=KOJI_TIMEOUT))
    tasks = b.listTasks(opts=dict(state=waiting_states))
    return tasks

def retrieve_repo_queue_length():
    b = koji.ClientSession(KOJI_URL, opts=dict(timeout=KOJI_TIMEOUT))
    length = b.repo.queryQueue(clauses=[['active', 'IS', True]],
                               opts={'countOnly': True})
    return length

def retrieve_repo_max_waiting_time():
    b = koji.ClientSession(KOJI_URL, opts=dict(timeout=KOJI_TIMEOUT))
    req = b.repo.queryQueue(clauses=[['active', 'IS', True]],
                            opts={'order': 'id', 'limit': 1})
    if not req:
        return 0

    req = req[0]
    return datetime.now().timestamp() - req['create_ts']


# This takes about 3s to generate and it changes very infrequently, so cache it.
# It is useful for making "saturation" metrics in promql.
@cache.cache_on_arguments()
def retrieve_hosts_by_channel():
    b = koji.ClientSession(KOJI_URL, opts=dict(timeout=KOJI_TIMEOUT))
    hosts = {}
    for idx, channel in CHANNELS.items():
        hosts[channel] = list(b.listHosts(channelID=idx, enabled=True))
    return hosts


def retrieve_task_load_by_channel():
    # Initialize return structure
    task_load = {}
    for idx, channel in CHANNELS.items():
        task_load[channel] = 0

    # Grab the channel to host mapping from in memory cache
    by_channel = retrieve_hosts_by_channel()

    hosts = b.listHosts(enabled=True)
    for host in hosts:
        for idx, channel in CHANNELS.items():
            if host['name'] in [h['name'] for h in by_channel[channel]]:
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


def calculate_overall_duration(task):
    if not task['completion_ts']:
        # Duration is undefined.
        # We could consider using `time.time()` as the duration, but that would produce durations
        # that change for incomlete tasks -- and changing durations like that is incompatible with
        # prometheus' histogram and counter model.  So - we just ignore tasks until they are
        # complete and have a final duration.
        raise IncompleteTask("Task is not yet complete.  Duration is undefined.")
    return task['completion_ts'] - task['create_ts']


def calculate_waiting_duration(task):
    if not task['start_ts']:
        # Duration is undefined because the task has not started yet.
        # We could consider using `time.time()` as the duration, but that would produce durations
        # that change for incomlete tasks -- and changing durations like that is incompatible with
        # prometheus' histogram and counter model.  So - we just ignore tasks until they are
        # started and have a final waiting duration.
        raise IncompleteTask("Task is not yet started.  Duration is undefined.")
    return task['start_ts'] - task['create_ts']


def calculate_in_progress_duration(task):
    if not task['completion_ts']:
        # Duration is undefined because the task has not completed yet.
        # We could consider using `time.time()` as the duration, but that would produce durations
        # that change for incomlete tasks -- and changing durations like that is incompatible with
        # prometheus' histogram and counter model.  So - we just ignore tasks until they are
        # complete and have a final duration.
        raise IncompleteTask("Task is not yet complete.  Duration is undefined.")
    if not task['start_ts']:
        # This shouldn't happen as a task with a completion_ts should always have a start_ts.
        # However, if a task is cancelled, or due to an unknown corner case, the start_ts may not
        # be set.
        raise IncompleteTask("Task completed but not started.  Duration is undefined.")
    return task['completion_ts'] - task['start_ts']


def find_applicable_buckets(duration):
    buckets = DURATION_BUCKETS + ["+Inf"]
    for bucket in buckets:
        if duration < float(bucket):
            yield bucket


def koji_task_duration_seconds(tasks, calculate_duration):
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


def koji_enabled_hosts_capacity(hosts):
    for channel in hosts:
        yield sum([h['capacity'] for h in hosts[channel]]), [channel]


def koji_hosts_last_update(hosts):
    results = []
    with b.multicall() as m:
        for channel, host_list in hosts.items():
            for host in host_list:
                # m.getLastHostUpdate returns a VirtualCall object.
                # It is handled later.
                results.append( [ m.getLastHostUpdate(host['id'], ts=True),
                                [ host['name'],
                                channel]])
    # Now, filter out any that have None for their value.
    results = [result for result in results if result[0].result is not None]
    return results


def koji_task_load(task_load):
    for channel, value in task_load.items():
        yield value, [channel]


def only(tasks, states):
    for task in tasks:
        state = task['state']
        if state in states:
            yield task


def scrape():
    global START
    today = datetime.utcnow().date()
    START = datetime.timestamp(datetime.combine(today, datetime.min.time()))

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

    koji_task_completions_total_family = CounterMetricFamily(
        'koji_task_completions_total', 'Count of all koji task completed', labels=TASK_LABELS
    )
    completed_tasks = only(tasks, states=completed_states)
    for value, labels in koji_tasks_total(completed_tasks):
        koji_task_completions_total_family.add_metric(labels, value)

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
    for buckets, duration_sum, labels in koji_task_duration_seconds(
        tasks, calculate_overall_duration
    ):
        koji_task_duration_seconds_family.add_metric(labels, buckets, sum_value=duration_sum)

    koji_task_waiting_duration_seconds_family = HistogramMetricFamily(
        'koji_task_waiting_duration_seconds',
        'Histogram of koji tasks durations while waiting',
        labels=TASK_LABELS,
    )
    for buckets, duration_sum, labels in koji_task_duration_seconds(
        tasks, calculate_waiting_duration
    ):
        koji_task_waiting_duration_seconds_family.add_metric(
            labels, buckets, sum_value=duration_sum
        )

    koji_task_in_progress_duration_seconds_family = HistogramMetricFamily(
        'koji_task_in_progress_duration_seconds',
        'Histogram of koji task durations while in-progress',
        labels=TASK_LABELS,
    )
    for buckets, duration_sum, labels in koji_task_duration_seconds(
        tasks, calculate_in_progress_duration
    ):
        koji_task_in_progress_duration_seconds_family.add_metric(
            labels, buckets, sum_value=duration_sum
        )


    koji_enabled_hosts_count_family = GaugeMetricFamily(
        'koji_enabled_hosts_count',
        'Count of all koji hosts by channel',
        labels=HOST_LABELS,
    )
    koji_enabled_hosts_capacity_family = GaugeMetricFamily(
        'koji_enabled_hosts_capacity',
        'Reported capacity of all koji hosts by channel',
        labels=HOST_LABELS,
    )

    koji_hosts_last_update_family = GaugeMetricFamily(
        'koji_hosts_last_update',
        'Gauge of last update from host',
        labels=BUILDER_LABELS,
    )

    koji_repo_queue_length = GaugeMetricFamily(
        'koji_repo_queue_length',
        'Number of active repo requests',
        retrieve_repo_queue_length(),
    )

    koji_repo_max_waiting_time = GaugeMetricFamily(
        'koji_repo_max_waiting_time',
        'Actual longest waiting tag for repo regeneration',
        retrieve_repo_max_waiting_time(),
    )

    hosts = retrieve_hosts_by_channel()

    # result_object is a VirtualCall object from the use of the MultiCallSession from the Koji API
    for result_object, labels in koji_hosts_last_update(hosts):
        koji_hosts_last_update_family.add_metric(labels, result_object.result)
    for value, labels in koji_enabled_hosts_count(hosts):
        koji_enabled_hosts_count_family.add_metric(labels, value)
    for value, labels in koji_enabled_hosts_capacity(hosts):
        koji_enabled_hosts_capacity_family.add_metric(labels, value)

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
            'koji_task_completions_total': koji_task_completions_total_family,
            'koji_in_progress_tasks': koji_in_progress_tasks_family,
            'koji_waiting_tasks': koji_waiting_tasks_family,
            'koji_task_duration_seconds': koji_task_duration_seconds_family,
            'koji_task_waiting_duration_seconds': koji_task_waiting_duration_seconds_family,
            'koji_task_in_progress_duration_seconds': koji_task_in_progress_duration_seconds_family,
            'koji_enabled_hosts_count': koji_enabled_hosts_count_family,
            'koji_enabled_hosts_capacity': koji_enabled_hosts_capacity_family,
            'koji_task_load': koji_task_load_family,
            'koji_hosts_last_update': koji_hosts_last_update_family,
            'koji_repo_queue_length': koji_repo_queue_length,
            'koji_repo_max_waiting_time': koji_repo_max_waiting_time,
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

    start_time = time.time()

    # Populate data before exposing over http
    scrape()
    start_http_server(8000)

    ready_time = time.time()
    print("Ready time: ", ready_time-start_time)

    while True:
        time.sleep(int(os.environ.get('KOJI_POLL_INTERVAL', '3')))
        scrape()
