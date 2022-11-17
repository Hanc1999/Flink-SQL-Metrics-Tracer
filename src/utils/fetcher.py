# libs fine
from time import sleep
from urllib import response
import requests
import json

import matplotlib.pyplot as plt
import numpy as np

# fetch functions: fetch (metrics) meta data
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

def fetch_job_meta(clusterUrl="http://172.28.176.179:8081", jobidx=0):
    # function: find current job's meta info sets
    # input: dashboard url, query job idx
    # output: meta info sets of the job, job id
    # first find current jobs
    response = requests.get("{}/jobs".format(clusterUrl))
    running_jobs = [j['id'] for j in json.loads(response.text)["jobs"][:] if j['status'] == 'RUNNING']
    if len(running_jobs) > 1 and jobidx == 0:
        print("[FETCH] Warning: More than one running job, will select the first one")
    # jobId = json.loads(response.text)["jobs"][jobidx]["id"]
    jobId = running_jobs[jobidx]
    # find vertices for the specific job
    response = requests.get("{}/jobs/{}".format(clusterUrl, jobId))
    meta = json.loads(response.text)
    return meta, jobId

def fetch_job_vertice_metrics(vers, jid, clusterUrl="http://172.28.176.179:8081", showurl=False):
    # input: vertice info sets, job id, dashboard url, 
    # output: vids of all vers, metrics of all vers (same order as in the arg:vers)
    vids = []
    vmetrics = []
    for ver in vers:
        vid = ver['id']
        the_url = "{}/jobs/{}/vertices/{}/subtasks/metrics".format(clusterUrl, jid, vid)
        if showurl:
            print(the_url)
        response = requests.get(the_url)
        metrics = json.loads(response.text)
        vids.append(vid)
        vmetrics.append(metrics)
    return vids, vmetrics

def fetch_job_metrics(jid, clusterUrl="http://172.28.176.179:8081", showurl=False):
    the_url = "{}/jobs/{}/metrics".format(clusterUrl, jid)
    if showurl:
        print(the_url)
    response = requests.get(the_url)
    metrics = json.loads(response.text)
    return metrics

def fetch_job_vertice_subtasks_metrics(vers, jid, clusterUrl="http://172.28.176.179:8081", showurl=False):
    # input: vertice info sets, job id, dashboard url, 
    # output: parallelism (num of subtasks), metrics of all sbtasks of those vertice
    sbmetrics = {}
    for ver in vers:
        vid = ver['id']
        vparallelism = ver['parallelism']
        sbmetrics[vid] = []
        for sbidx in range(vparallelism):
            the_url = "{}/jobs/{}/vertices/{}/subtasks/{}/metrics".format(clusterUrl, jid, vid, sbidx)
            if showurl:
                print(the_url)
            response = requests.get(the_url)
            metrics = json.loads(response.text)
            sbmetrics[vid].append(metrics)
    return vparallelism, sbmetrics

# search functions: search for wanted metrics according to meta infos
def help_find_query_field(wanted, can_metrics, omit_operator_level=True):
    # helper, generate the metrics values query field
    query_field = ""
    all_wanted = False
    if wanted == []: # empty means wanting everything
        all_wanted = True
    # loop and filter wanted metrics
    for metric in can_metrics:
        if omit_operator_level:
            if '[' in metric["id"]: continue
        test_flag = False
        if not all_wanted:
            for m in wanted:
                if False in [m_i in metric["id"] for m_i in m]:
                    continue
                else:
                    test_flag = True
                    break
        if test_flag or all_wanted:
            query_field += metric["id"] + ","
    return query_field

def search_job_metrics_values(metricnames, jobId, jmetrics,
            clusterUrl="http://172.28.176.179:8081", showurl=False, omit_operator_level=True):
    # search metrics for a single job
    # input: metricnames: wanted metric names, job id, jmetrics
    # output: res: a dict of interest metrics in a single job
    query_field = help_find_query_field(metricnames, jmetrics, omit_operator_level)
    the_url = "{}/jobs/{}/metrics?get={}".format(clusterUrl, jobId, query_field[:-1])
    if showurl:
        print(the_url)
    response = requests.get(the_url)
    metric_values = json.loads(response.text)
    return metric_values

def search_job_vertex_metrics_values(metricnames, jobId, vertexId, vmetrics,
            clusterUrl="http://172.28.176.179:8081", showurl=False, omit_operator_level=True):
    # search metrics for a single vertex
    # input: metricnames: wanted metric names, job id, vertex id, vmetrics
    # output: res: a dict of interest metrics in a single vertex
    query_field = help_find_query_field(metricnames, vmetrics, omit_operator_level)
    the_url = "{}/jobs/{}/vertices/{}/subtasks/metrics?get={}".format(clusterUrl, jobId, vertexId, query_field[:-1])
    if showurl:
        print(the_url)
    response = requests.get(the_url)
    metric_values = json.loads(response.text)
    return metric_values

def search_job_vertex_subtasks_metrics_values(metricnames, jobId, vertexId, sbtaskIdx, sbmetrics,
            clusterUrl="http://172.28.176.179:8081", showurl=False, omit_operator_level=True):
    # search metrics for a single subtask
    # input: metricnames: wanted metric names, job id, vertex id, subtask index, sbmetrics
    # output: res: a dict of interest metrics in a single subtask
    query_field = help_find_query_field(metricnames, sbmetrics, omit_operator_level)
    the_url = "{}/jobs/{}/vertices/{}/subtasks/{}/metrics?get={}".format(clusterUrl, jobId, vertexId, sbtaskIdx, query_field[:-1])
    if showurl:
        print(the_url)
    response = requests.get(the_url)
    metric_values = json.loads(response.text)
    return metric_values

# statistic functions: Continuous metrics statistics based on timestamps
def help_make_res(query_res):
    res = {}
    for metric in query_res:
        mkey = metric['id']
        res[mkey] = {}
        for sk, sv in metric.items():
            if sk == 'id': continue
            res[mkey][sk] = sv
    return res

def sta_job_vertice_metric_values(metricnames, jobId, vertexIds,  vmetricses, interval=10, itrs=100,
            clusterUrl="http://172.28.176.179:8081", showurl=False):
    ### lagency ###
    # collect all interest metrics for vertices in a job by time series
    # (omits all operator-level metrics)
    print('Collecting metrics:', end='')
    res = {}
    for i in range(itrs):
        for v_idx in range(len(vertexIds)):
            # for each vertex collect:
            vertexId = vertexIds[v_idx]
            vmetrics = vmetricses[v_idx]
            if res.get(vertexId) is None: res[vertexId] = {}
            # fetch metric values related to our search info
            current_res = search_job_vertex_metrics_values(metricnames, jobId, vertexId,  vmetrics, clusterUrl="http://172.28.176.179:8081", showurl=False)
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

def sta_job_everything(jobId, jmeta, interval=6, itrs=20,
            clusterUrl="http://172.28.176.179:8081", showurl=False):
    # collect all interest metrics for the job itself, vertices, subtasks in a job by time series
    # the wanted here is a dict, identifies which metrics you want in each level
    # returns: a list of samples, each sample contains full-level info for a job
    # (omits all operator-level metrics)
    print('Collecting metrics:', end='')
    res = [] # a list of dict-samples

    for i in range(itrs):
        # Job-level
        job_meta = jmeta #fin
        job_metrics = fetch_job_metrics(jobId)
        job_data = {} #fin
        vers = job_meta['vertices'] # vertice meta
        # make wanted for job: latencies
        source_vertex_id = None
        sink_vertex_id = None
        for ver in vers:
            if 'Source' in ver['name']: source_vertex_id = ver['id']
            elif 'Sink' in ver['name']: sink_vertex_id = ver['id']
        for latency_type in ['p90', 'p95', 'mean']:
            # multi-time requesting, otherwise bad request error
            latency_metric_values = search_job_metrics_values([['latency', source_vertex_id, sink_vertex_id, latency_type],], jobId, job_metrics)
            job_data['latency_'+latency_type] = help_make_res(latency_metric_values)

        # Vertice-level
        vertice_data = [] # k: vid, v: metric values, fin
        _, vers_metrics = fetch_job_vertice_metrics(vers, jobId)
        _, sbs_metrics = fetch_job_vertice_subtasks_metrics(vers, jobId)

        for ver_i in range(len(vers)):
            # for each vertex collect:
            vertex_data = {} # will append into the vertice data
            ver = vers[ver_i] # these steps guarantees the order of vertices in vertice-level is same as in job meta
            vertexId = ver['id']
            vparallel = ver['parallelism']
            
            vmetrics = vers_metrics[ver_i]
            # make wanted
            ver_wanted = [[vm['id']] for vm in vmetrics if '[' not in vm['id']]
            
            current_res = search_job_vertex_metrics_values(ver_wanted, jobId, vertexId, vmetrics)
            
            vertex_data['vertex_id'] = vertexId
            vertex_data['vertex_level_metrics'] = help_make_res(current_res)

        # Sub-task Level
            sb_data = []
            for sb_idx in range(vparallel):
                sbm = sbs_metrics[vertexId][sb_idx]
                # make wanted
                sb_wanted = [[m['id']] for m in sbm if '[' not in m['id']]
                
                sbmvs = search_job_vertex_subtasks_metrics_values(sb_wanted, jobId, vertexId, sb_idx, sbm)
                sb_data.append(help_make_res(sbmvs))
            
            vertex_data['sub_level_metrics'] = sb_data # vertex data assemble done, append
            vertice_data.append(vertex_data)

        # assemble the whole sample
        sample = {}
        sample['job_id'] = jobId
        sample['job_meta_info'] = job_meta
        sample['job_level_metrics'] = job_data
        sample['vertice_metrics'] = vertice_data

        res.append(sample)

        if i % max(1, (itrs//20)) == 0:
            print('|',end='')

        sleep(interval)

    return res