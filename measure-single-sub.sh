#!/bin/bash

ARG="${1} ${2} ${3} ${4} ${5} ${6} ${7} ${8} ${9} ${10} ${11} ${12} ${13} ${14} ${15} ${16} ${17} ${18} ${19} ${20}"
EXECFILE="subscriber.go"
EXECFILE_DIR="cmd/single-subscriber/main.go"
LOGFILE_DIR="logs/single/sub/$(python3 -c "from datetime import datetime as dt;print(dt.now().strftime('%Y-%m-%d/%H'))")"
LOGFILE="${LOGFILE_DIR}/$(python3 -c "from datetime import datetime as dt;print(dt.now().strftime('%Y%m%d-%H%M%S-%f'))").log"
LOGFILE_SIG="logs/`hostname`.logs.sig"
SIG_CMD="md5sum"
if [ `uname` = "Darwin" ]; then
	# MacOS を想定
	SIG_CMD="md5"
fi

if [ ! -d ${LOGFILE_DIR} ]; then
    mkdir -p ${LOGFILE_DIR}
fi
SELF_MD5SUM=`${SIG_CMD} ${0}`
cd ${EXECFILE_DIR}
EXECFILE_MD5SUM=`${SIG_CMD} ${EXECFILE}`
go run ${EXECFILE} ${ARG} | tee -a ../${LOGFILE}
cd -
sleep 3  # publisher 側のスクリプトが終わるのを待つ
LOGFILE_SIG_MD5SUM=`${SIG_CMD} ${LOGFILE_SIG}`
LOGFILE_SIG_LEN=`cat ${LOGFILE_SIG} | wc -l`
CLIENT_NUM=`cat ${LOGFILE} | grep -oE "OPTION Client num[ ]+:[ ]+[0-9]+" | sed -r "s/OPTION Client num[ ]+:[ ]+([0-9]+)/\1/g"`

# ログファイルの統計結果を追記、表示
echo "" | tee -a ${LOGFILE} | tee -a ${LOGFILE}
echo "###################### Added by ${0} ######################" | tee -a ${LOGFILE}
echo "${SIG_CMD} ${EXECFILE_MD5SUM}" | tee -a ${LOGFILE}
echo "${SIG_CMD} ${SELF_MD5SUM}" | tee -a ${LOGFILE}
echo "${SIG_CMD} ${LOGFILE_SIG_MD5SUM}" | tee -a ${LOGFILE}
echo "Signature file length             : ${LOGFILE_SIG_LEN} (${LOGFILE_SIG})"
echo "Current directory                 : `pwd`"
cat ${LOGFILE} | grep -oE "OPTION .+$" | tee -a ${LOGFILE}
echo "MQTT Publish error num            : `cat ${LOGFILE} | grep 'MQTT Publish error' | wc -l`" | tee -a ${LOGFILE}
echo "MQTT Connect error num            : `cat ${LOGFILE} | grep 'MQTT Connect error' | wc -l`" | tee -a ${LOGFILE}
cat ${LOGFILE} | grep -oE "Average : [0-9]+(\.[0-9]+)? \[ms\] \[n=[0-9]+\] \(ID: .+\)$" | \
sed -r "s/Average : ([0-9]+(\.[0-9]+)?) \[ms\] \[n=([0-9]+)\] \(ID: (.+)\)$/\1 \3 \4/g" | \
awk -v client_num=${CLIENT_NUM} '{
    if($1 != 0){
        x_max[$3] = $1
        x_min[$3] = $1
        n_max[$3] = $2
        n_min[$3] = $2
        x[$3][NR] = $1
        n[$3][NR] = $2
    }
}
END {
    if(length(x) == 0) {
        print "===================== ID: ====================="
        print "Latency average                   : --- [ms] [n=0]"
        print "Latency max                       : --- [ms]"
        print "Latency min                       : --- [ms]"
        print "Latency variance                  : ---"
        print "Latency standard deviation        : ---"
        print "Message sum                       : --- [msg] [n=0]"
        print "Message sum per client            : --- [msg] [client_num=0]"
        print "Message average                   : --- [msg/sec] [n=0]"
        print "Message max                       : --- [msg/sec]"
        print "Message min                       : --- [msg/sec]"
        print "Message variance                  : ---"
        print "Message standard deviation        : ---"
        print "-----------------------------------------------"
        exit
    }
    for(id in x){
        for(i in x[id]){
            sum_x[id] += x[id][i]
            sum_n[id] += n[id][i]
            if (x_max[id] < x[id][i]) {
                x_max[id] = x[id][i]
            }
            if (x_min[id] > x[id][i]) {
                x_min[id] = x[id][i]
            }
            if (n_max[id] < n[id][i]) {
                n_max[id] = n[id][i]
            }
            if (n_min[id] > n[id][i]) {
                n_min[id] = n[id][i]
            }
        }
    }
    for(id in x){
        m_x[id] = sum_x[id] / length(x[id])
        m_n[id] = sum_n[id] / length(n[id])
    }
    for(id in x){
        for(i in x[id]){
            sum_dx2[id] += (x[id][i] - m_x[id]) ^ 2
            sum_dn2[id] += (n[id][i] - m_n[id]) ^ 2
        }
    }
    for(id in x){
        statistics[id][00] = " ID: " id " "
        statistics[id][01] = "Latency average                   : " m_x[id] " [ms] [n=" length(x[id]) "]"
        statistics[id][02] = "Latency max                       : " x_max[id] " [ms]"
        statistics[id][03] = "Latency min                       : " x_min[id] " [ms]"
        statistics[id][04] = "Latency variance                  : " sum_dx2[id] / length(x[id])
        statistics[id][05] = "Latency standard deviation        : " sqrt(sum_dx2[id] / length(x[id]))
        statistics[id][06] = "Message sum                       : " sum_n[id] " [msg] [n=" length(x[id]) "]"
        if (client_num > 0) {
            statistics[id][07] = "Message sum per client            : " sum_n[id] / client_num " [msg] [client_num=" client_num "]"
        }
        statistics[id][08] = "Message average                   : " m_n[id] " [msg/sec] [n=" length(x[id]) "]"
        statistics[id][09] = "Message max                       : " n_max[id] " [msg/sec]"
        statistics[id][10] = "Message min                       : " n_min[id] " [msg/sec]"
        statistics[id][11] = "Message variance                  : " sum_dn2[id] / length(x[id])
        statistics[id][12] = "Message standard deviation        : " sqrt(sum_dn2[id] / length(x[id]))
    }
    maxStrLen = 0
    for(id in statistics){
        for(i in statistics[id]){
            if(maxStrLen < length(statistics[id][i])){
                maxStrLen = length(statistics[id][i])
            }
        }
    }
    for(id in statistics){
        for(i = length(statistics[id][0]); i < maxStrLen; i++){
            if((i % 2) == 0){
                statistics[id][0] = (statistics[id][0] "=")
            }else{
                statistics[id][0] = ("=" statistics[id][0])
            }
        }
    }
    for(id in statistics){
        for(i in statistics[id]){
            print statistics[id][i]
        }
    }
    ENDLINE = ""
    for(i = 0; i < maxStrLen; i++){
        ENDLINE = (ENDLINE "-")
    }
    print ENDLINE
}' | tee -a ${LOGFILE}

# システム情報をログファイルに保存（標準出力には表示しない）
echo "" >> ${LOGFILE}
echo "#### System Info ####" >> ${LOGFILE}
if [ `uname` = "Darwin" ]; then
    system_profiler SPHardwareDataType >> ${LOGFILE}
else
    cat /etc/lsb-release >> ${LOGFILE}
    cat /proc/cpuinfo >> ${LOGFILE}
    cat /proc/meminfo >> ${LOGFILE}
fi

# 生成したログファイルのハッシュを保存
echo "${SIG_CMD} `${SIG_CMD} ${LOGFILE}`" >> ${LOGFILE_SIG}
