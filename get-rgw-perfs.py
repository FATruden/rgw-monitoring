#!/bin/env python

import json
import glob
import os
import subprocess
import sys


SOCK_DIR = "/var/run/ceph"
RGW_SOCK_PREF = "ceph-client.*"
SAVE_PATH = "/tmp"

MONITORING_KEYS = [
    "req",
    "failed_req",
    "get",
    "get_b",
    "put",
    "put_b",
    "qlen",
    "qactive",
    "cache_hit",
    "cache_miss",
]


def get_rgw_instances():
    """
    Reterns list of rgw/rwb for monitoring like:
    [{'asock': '/var/run/ceph/ceph-client.rgw.a.asok', 'type': 'rgw'}]
    """

    active_sockets = glob.glob(os.path.join(SOCK_DIR, RGW_SOCK_PREF))

    instances = []
    for sock in active_sockets:
        instances.append(
            {"type": sock.split(".")[1], "id": sock.split(".")[2], "asock": sock})

    return instances


def get_metrics(admin_sock):
    """Returns JSON of all RGW/RWB metrics."""

    try:
        raw_metrics = subprocess.Popen(["ceph", "--format", "json",
            "--admin-daemon", admin_sock, "perf", "dump"], stdout=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        sys.exit("Error: {0}".format(e))

    try:
        return json.loads(raw_metrics.stdout.read())
    except ValueError as e:
        sys.exit("Error: {0}".format(e))


def write_result(filename, result):
    """Write metric to file."""

    f = open(filename, 'w')
    f.write(str(result))
    f.close()


def get_latency(metrics, rgw_type, rgw_name, latency_type):
    """Calculate latency betwen checks."""

    metric_file = "{0}/{1}.{2}".format(SAVE_PATH, rgw_type, latency_type)
    last_metrics_file = "{0}/last_{1}.{2}".format(
        SAVE_PATH, rgw_type, latency_type)

    current_obj_count = float(metrics[rgw_name][latency_type]["avgcount"])
    current_lat_sum = float(metrics[rgw_name][latency_type]["sum"])

    if os.path.isfile(last_metrics_file):
        last_metrics = open(last_metrics_file, 'r')
        last_obj_count, last_lat_sum = last_metrics.read().split()
        last_metrics.close()

        obj_count = current_obj_count - float(last_obj_count)
        lat_sum = current_lat_sum - float(last_lat_sum)

        if obj_count > 0.0:
            latency = round(lat_sum / obj_count, 3)
            write_result(metric_file, latency)
        else:
            write_result(metric_file, 0)
    else:
        latency = round(current_lat_sum / current_obj_count, 3)
        write_result(metric_file, latency)

    write_result(last_metrics_file, "{0} {1}".format(
        current_obj_count, current_lat_sum))


def main():

    for rgw in get_rgw_instances():
        rgw_name = "client.{0}.{1}".format(rgw["type"], rgw["id"])
        metrics = get_metrics(rgw["asock"])

        for key in MONITORING_KEYS:
            metric = metrics[rgw_name][key]
            filename = "{0}/{1}.{2}".format(SAVE_PATH, rgw["type"], key)
            write_result(filename, metric)

        get_latency(metrics, rgw["type"], rgw_name, "get_initial_lat")

        # RWB doesn't have put metrics
        if rgw["type"] == "rgw":
            get_latency(metrics, rgw["type"], rgw_name, "put_initial_lat")


if __name__ == "__main__":
    main()
