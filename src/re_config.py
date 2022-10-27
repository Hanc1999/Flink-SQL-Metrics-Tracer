import sys

def modify_nexmark_conf(origin_conf_f_path, new_conf_f_path, tps=10000):
    tps_signal = "nexmark.workload.suite.100m.tps:"
    with open(origin_conf_f_path, 'r') as o_conf, open(new_conf_f_path, 'w') as n_conf:
        for line in o_conf:
            if tps_signal in line:
                line2 = tps_signal + ' ' + str(tps) + "\n"
                n_conf.write(line2)
            else:
                n_conf.write(line)

def modify_flink_conf(origin_conf_f_path, new_conf_f_path, parallel=4):
    parallel_signal = "parallelism.default:"
    with open(origin_conf_f_path, 'r') as o_conf, open(new_conf_f_path, 'w') as n_conf:
        for line in o_conf:
            if parallel_signal in line:
                line2 = parallel_signal + ' ' + str(parallel) + "\n"
                n_conf.write(line2)
            else:
                n_conf.write(line)

def main():
    # modifies the conf file according to the input hyper-p
    # argv:   prog, singal, o_path, n_path, conf_para
    # argidx: 0,    1,      2,      3,      4,
    cmdargs = sys.argv
    signal = cmdargs[1]
    o_path = cmdargs[2]
    n_path = cmdargs[3]
    conf_para = cmdargs[4]
    if signal == 'nexmark':
        modify_nexmark_conf(o_path, n_path, tps=int(conf_para))
    elif signal == 'flink':
        modify_flink_conf(o_path, n_path, parallel=int(conf_para))
    else:
        print("[Conf] Error: not supported conf modifee.")

main()