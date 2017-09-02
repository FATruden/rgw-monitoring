#!/bin/env python

import json
import os
import platform
import subprocess
import sys


hostname = platform.node().split(".")[0]
rgw_name = "client.radosgw.{0}".format(hostname)
rgw_sock = "/var/run/ceph/ceph-{0}.asok".format(rgw_name)
save_path = "/tmp"
file_prefix = "rgw"

keys = [
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


def get_metrics():
    """Get all metrics from RGW and return their as JSON"""

    try:
        raw_metrics = subprocess.Popen(["ceph",
            "--format", "json",
            "--admin-daemon", rgw_sock,
            "perf", "dump"], stdout=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        sys.exit("Error: {0}".format(e))

    try:
        return json.loads(raw_metrics.stdout.read())
    except ValueError as e:
        sys.exit("Error: {0}".format(e))


def write_res(filename, result):
    """Write metric to file."""

    f = open(filename, 'w')
    f.write(str(result))
    f.close()


def get_latency(metrics, latency_type):
    """Calculate latency betwen checks"""

    metric_file = "{0}/{1}.{2}".format(save_path, file_prefix, latency_type)
    last_metrics_file = "{0}/last_{1}.{2}".format(save_path,
        file_prefix, latency_type)

    current_obj_count = float(metrics[rgw_name][latency_type]["avgcount"])
    current_lat_sum = float(metrics[rgw_name][latency_type]["sum"])

    if os.path.isfile(last_metrics_file):
        last_metrics = open(last_metrics_file, 'r')
        last_obj_count, last_lat_sum = last_metrics.read().split()
        last_metrics.close()

        obj_count = current_obj_count - float(last_obj_count)
        lat_sum = current_lat_sum - float(last_lat_sum)

        if obj_count > 0.0:
            latency = lat_sum / obj_count
            write_res(metric_file, latency)
            write_res(last_metrics_file, "{0} {1}".format(
                current_obj_count, current_lat_sum))

    else:
        write_res(last_metrics_file, "{0} {1}".format(
            current_obj_count, current_lat_sum))
        write_res(metric_file, 0)


def main():

    metrics = get_metrics()

    for key in keys:
        metric = metrics[rgw_name][key]
        filename = "{0}/{1}.{2}".format(save_path, file_prefix, key)
        write_res(filename, metric)

    get_latency(metrics, "get_initial_lat")
    get_latency(metrics, "put_initial_lat")


if __name__ == "__main__":
    main()
