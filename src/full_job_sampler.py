import csv
import json
import os
import sys

from utils.fetcher import *
from utils.file_maker import *

def monitor_the_job(qid, parallel, tps):

    # fetch vertex meta info
    rows = [] # data to return
    run_confs = {'qid': qid,
                 'parallel': parallel,
                 'tps': tps
                 }

    jmeta, jid = fetch_job_meta()
    samples = sta_job_everything(jid, jmeta, interval=10, itrs=10,)

    # assemble samples with run_configs
    for sample in samples:
        rows.append([run_confs, sample])

    return rows

def main():
    # Monitor vertex-level metrics for the running job, 
    # append data-rows into .csv file as a tabular dataset
    # argv:     prog,   qid,    parallel,   tps,    o_path,
    # argidx:   0,      1,      2,          3,      4,
    
    # parser args
    cmdargs = sys.argv
    qid = int(cmdargs[1][1:]) # q8 -> (int)8
    parallel = int(cmdargs[2])
    tps = int(cmdargs[3])
    o_path = cmdargs[4]

    sleep(25)   # wait metric servers warmup

    # run the sampler
    header = ['run_confs', 'job_metrics']
    data = monitor_the_job(qid, parallel, tps)
    # write to the o_file
    if os.path.isfile(o_path):
        header = None
    write_csv(o_path, data, header)

if __name__ == "__main__":
    main()