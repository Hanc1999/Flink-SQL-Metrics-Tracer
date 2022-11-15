USAGE="Usage: run all queries safely and clean"

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

PYTHON_HOME=/home/flink/workspace/yimin/Flink-SQL-Metrics-Tracer/src
FLINK_CONF_DIR=$FLINK_HOME/conf
DATA_O_PATH=/home/flink/workspace/yimin/datasets/nexmark_full_job_Nov14/test
TMP_PATH=/tmp


for parallel in 2 3 4 6 8 12 16
do
    # reconfig flink-conf.yaml
    python3 "$PYTHON_HOME/re_config.py" flink "$FLINK_CONF_DIR/conf_bak/flink-conf.yaml" "$FLINK_CONF_DIR/flink-conf.yaml" $parallel

    for tps_scalar in 5000 10000 20000 50000 100000 150000
    do
        # make $tps
        tps=$(($tps_scalar * $parallel))
        # reconfig nexmark.yaml
        python3 "$PYTHON_HOME/re_config.py" nexmark "$NEXMARK_CONF_DIR/conf_bak/nexmark.yaml" "$NEXMARK_CONF_DIR/nexmark.yaml" $tps
        
        for QUERY in q4 q5 q7 q9
        do
            log=$NEXMARK_LOG_DIR/nexmark-flink.log
            log_setting=(-Dlog.file="$log" -Dlog4j.configuration=file:"$NEXMARK_CONF_DIR"/log4j.properties -Dlog4j.configurationFile=file:"$NEXMARK_CONF_DIR"/log4j.properties)

            echo "$0" "$1" "$QUERY" "${log_setting[@]}" "$NEXMARK_HOME" "$FLINK_HOME"

            # parallel run following 2 cmds: run job & metric monitor
            java "${log_setting[@]}" -cp "$NEXMARK_HOME/lib/*:$FLINK_HOME/lib/*" com.github.nexmark.flink.Benchmark --location "$NEXMARK_HOME" --queries "$QUERY"  &
            python3 "$PYTHON_HOME/full_job_sampler.py" $QUERY $parallel $tps $DATA_O_PATH/test.csv
            # then barrier
            wait

            # cleaning
            echo Sampling done, Cleaning logs, checkpoints and tmps
            # cleaning 1: log files
            rm $FLINK_HOME/log/tmp_job_*
            # cleaning 2: checkpoint dirs and files
            rm -r $FLINK_HOME/checkpoint-dir/*
            # cleaning 3: tmp dirs and files
            rm -r $TMP_PATH/tm_127.0.0.1*
            rm -r $TMP_PATH/job_*uuid*
            rm $TMP_PATH/flink-table-*.jar

            # reset the environment
            "$NEXMARK_HOME/bin/shutdown_cluster.sh"
            "$FLINK_HOME/bin/stop-cluster.sh"
            "$FLINK_HOME/bin/start-cluster.sh"
            "$NEXMARK_HOME/bin/setup_cluster.sh"

        done
    done
done