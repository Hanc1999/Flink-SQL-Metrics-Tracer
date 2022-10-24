# libs fine
from time import sleep
from urllib import response
import requests
import json

import matplotlib.pyplot as plt
import numpy as np

def fetch_job_vertices(clusterUrl="http://172.28.176.179:8081", jobidx=0):
    # function: find current job's vertices info sets
    # input: dashboard url, query job idx
    # output: vertices info sets of the job, job id
    # first find current jobs
    response = requests.get("{}/jobs".format(clusterUrl))
    running_jobs = [j['id'] for j in json.loads(response.text)["jobs"][:] if j['status'] == 'RUNNING']
    if len(running_jobs) > 1 and jobidx == 0:
        print("[FETCH] Warning: More than one running job, will select the first one")
    # jobId = json.loads(response.text)["jobs"][jobidx]["id"]
    jobId = running_jobs[jobidx]
    # find vertices for the specific job
    response = requests.get("{}/jobs/{}".format(clusterUrl, jobId))
    vertices = json.loads(response.text)["vertices"]
    return vertices, jobId # vertices has detailed informations including like name, max_parallelism, parallelism

def fetch_job_vertice_metrics(vers, jid, clusterUrl="http://172.28.176.179:8081", showurl=False):
    # input: vertice info sets, job id, dashboard url, 
    # output: vids of all vers, metrics of all vers
    vids = []
    vmetrics = []
    for ver in vers:
        vid = ver['id']
        if showurl:
            print("{}/jobs/{}/vertices/{}/subtasks/metrics".format(clusterUrl, jid, vid))
        response = requests.get("{}/jobs/{}/vertices/{}/subtasks/metrics".format(clusterUrl, jid, vid))
        metrics = json.loads(response.text)
        vids.append(vid)
        vmetrics.append(metrics)
    return vids, vmetrics

def search_job_vertice_metric_values(metricnames, jobId, vertexId,  vmetrics,
            clusterUrl="http://172.28.176.179:8081", showurl=False):
    # search metrics for a single vertice
    # input: metricnames: wanted metric names, job id, vertice id, vmetrics: metrics this vertive has
    # output: res: a dict of interest metrics in a single vertice
    res = {}
    for m in metricnames:
        query_field = ""
        for metric in vmetrics:
            test_flag = True
            for m_i in m:
                if m_i not in metric["id"]:
                    test_flag = False
                    break
            if test_flag:
                query_field += metric["id"] + ","
        if showurl:
            print("{}/jobs/{}/vertices/{}/subtasks/metrics?get={}\n"
                              .format(clusterUrl, jobId, vertexId, query_field[:-1]))
        response = requests.get("{}/jobs/{}/vertices/{}/subtasks/metrics?get={}"
                              .format(clusterUrl, jobId, vertexId, query_field[:-1]))
        res["".join(m)] = json.loads(response.text)
    return res

def sta_job_vertice_metric_values(metricnames, jobId, vertexIds,  vmetricses, interval=10, itrs=100,
            clusterUrl="http://172.28.176.179:8081", showurl=False):
    # collect all interest metrics for vertices in a job by time series
    print('Collecting metrics:', end='')
    res = {}
    for i in range(itrs):
        for v_idx in range(len(vertexIds)):
            # for each vertex collect:
            vertexId = vertexIds[v_idx]
            vmetrics = vmetricses[v_idx]
            if res.get(vertexId) is None: res[vertexId] = {}
            # fetch metric values related to our search info
            current_res = search_job_vertice_metric_values(metricnames, jobId, vertexId,  vmetrics, clusterUrl="http://172.28.176.179:8081", showurl=False)
            # record to the res-record
            for k,v in current_res.items():
                # v here is the list of metrics containing the searching keyword
                # k here is the user search key
                for m in v:
                    m_id = m.get("id", None)
                    if m_id is None: continue
                    if res[vertexId].get(m_id) is None: res[vertexId][m_id]={}
                    for m_k, m_v in m.items():
                        if m_k == "id": continue
                        # m_id: metric name, m_k: metric subitem like sum/max/avg
                        if res[vertexId][m_id].get(m_k) is None: res[vertexId][m_id][m_k] = []
                        res[vertexId][m_id][m_k].append(m_v)
        if i % max(1, (itrs//20)) == 0:
            print('|',end='')
        sleep(interval)
    return res