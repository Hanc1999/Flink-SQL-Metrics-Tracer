from utils.fetcher import *

def monitor_the_job():
    vertices, jid = fetch_job_vertices()
    vertices_names = [v['name'] for v in vertices]
    vids, vmetricses = fetch_job_vertice_metrics(vertices, jid)
    res = sta_job_vertice_metric_values(["busy", "numBytesInPerSecond"], jid, vids, vmetricses, interval=5, itrs=2)
    print(res)

sleep(30)
monitor_the_job()