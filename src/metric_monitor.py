import csv
import json
import os
import sys

from utils.fetcher import *


def make_vertex_data(wanted, qid, vi, vname, parallel, in_stream_num, out_stream_num, v_monitored_metrics):
    # debug:
    # print(qid, vi, vname, parallel, in_stream_num, out_stream_num, v_monitored_metrics)
    cols = []
    for w_m in wanted:
        col = v_monitored_metrics.get(w_m, None)
        if col is None:
            print('[fetcher] Warning: No metrics monitered named ', w_m)
            continue
        col = col.get('avg', None)
        if col is None:
            print('[fetcher] Warning: Avg not supported by metrics ', w_m)
            continue
        cols.append(col)

    # length checking
    check_len_list = [len(col) for col in cols]
    for l in check_len_list:
        assert l == check_len_list[0], '[fetcher] Error: cols lens not aligned'

    rows = []
    for i in range(check_len_list[0] if len(check_len_list)>=1 else 0):  # assumes v_monitored metrics non-empty
        row = [qid, vi, vname, parallel, in_stream_num, out_stream_num]
        tmp = [col[i] for col in cols]
        row += tmp
        rows.append(row)

    return rows

def make_header(wanted):
    header = ['query_idx',
                'vertex_idx',
                'vertex_name',
                'parallelism',
                'in_stream_num',
                'out_stream_num',]
    for k in wanted:
        header.append(k)
    return header

def write_csv(o_path, data, header=None):
    with open(o_path,'a') as o_fp:
        writer = csv.writer(o_fp)
        if header is not None:
            writer.writerow(header)
        for row in data:
            writer.writerow(row)

def monitor_the_job(qid, wanted, io_stream_info):

    # fetch vertex meta info
    vertices, jid = fetch_job_vertices()
    vertices_names = [v['name'] for v in vertices]
    vertices_parallels = [v['parallelism'] for v in vertices]
    
    vids, vmetricses = fetch_job_vertice_metrics(vertices, jid)
    monitored_metrics = sta_job_vertice_metric_values(wanted, jid, vids, vmetricses, interval=6, itrs=20)

    data = []
    v_num = len(vids)
    assert v_num == len(monitored_metrics), '[fetcher] Error: Monitored length != Vids length'
    for vi in range(v_num):
        vid = vids[vi]  # vid is sys-generated token, not index; vi is index
        new_rows = make_vertex_data(wanted,
                                    qid, 
                                    vi, 
                                    vertices_names[vi],
                                    vertices_parallels[vi],
                                    io_stream_info['in'][str(qid)][str(vi)],
                                    io_stream_info['out'][str(qid)][str(vi)],
                                    monitored_metrics[vid])
        data += new_rows
    return data

def main():
    # Monitor vertex-level metrics for the running job, 
    # append data-rows into .csv file as a tabular dataset
    # argv:     prog,   qid,    o_path,
    # argidx:   0,      1,      2,
    cmdargs = sys.argv
    qid = int(cmdargs[1][1:]) # q8 -> (int)8
    o_path = cmdargs[2]

    sleep(25)   # wait metric servers warmup

    # identify the wanted metrics here
    # TODO: move this part to a conf file
    wanted = [  'numBytesOutPerSecond',
                'numBuffersOutPerSecond',
                'numBytesInPerSecond',
                'numRecordsOutPerSecond',
                'numRecordsInPerSecond',
                'softBackPressuredTimeMsPerSecond',
                'hardBackPressuredTimeMsPerSecond',
                'idleTimeMsPerSecond',
                'busyTimeMsPerSecond',
                'backPressuredTimeMsPerSecond']
    
    # laod i/o stream info
    io_stream_info_f_path = "/home/flink/workspace/yimin/Flink-SQL-Metrics-Tracer/src/io_stream_info.json"   # TODO:replace by conf file
    with open(io_stream_info_f_path, 'r') as fp:
        io_stream_info = json.load(fp)

    # run the monitor
    data = monitor_the_job(qid, wanted, io_stream_info)
    # write to the o_file
    header = None
    if not os.path.isfile(o_path):
        header = make_header(wanted)
    write_csv(o_path, data, header)

if __name__ == "__main__":
    main()