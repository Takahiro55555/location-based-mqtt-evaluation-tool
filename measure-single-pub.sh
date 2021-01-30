#!/bin/bash

ARG="${1} ${2} ${3} ${4} ${5} ${6} ${7} ${8} ${9} ${10} ${11} ${12} ${13} ${14} ${15} ${16} ${17} ${18} ${19} ${20}"
EXECFILE="publisher.go"
EXECFILE_DIR="cmd/single-publisher/main.go"
LOGFILE_DIR="logs/single/pub/$(python3 -c "from datetime import datetime as dt;print(dt.now().strftime('%Y-%m-%d/%H'))")"
LOGFILE="${LOGFILE_DIR}/$(python3 -c "from datetime import datetime as dt;print(dt.now().strftime('%Y%m%d-%H%M%S-%f'))").log"
LOGFILE_SIG="logs/`hostname`.logs.sig"
SIG_CMD="md5sum"
if [ `uname` = "Darwin" ]; then
	SIG_CMD="md5"  # MacOS を想定
fi

if [ ! -d ${LOGFILE_DIR} ]; then
    mkdir -p ${LOGFILE_DIR}
fi
SELF_MD5SUM=`${SIG_CMD} ${0}`
cd ${EXECFILE_DIR}
EXECFILE_MD5SUM=`${SIG_CMD} ${EXECFILE}`
go run ${EXECFILE} ${ARG} | tee -a ../${LOGFILE}
cd -
LOGFILE_SIG_MD5SUM=`${SIG_CMD} ${LOGFILE_SIG}`
LOGFILE_SIG_LEN=`cat ${LOGFILE_SIG} | wc -l`
sleep 1

# ログファイルの統計結果を追記、表示
echo "" | tee -a ${LOGFILE} | tee -a ${LOGFILE}
echo "########### Added by ${0} ###########" | tee -a ${LOGFILE}
echo "${SIG_CMD} ${EXECFILE_MD5SUM}" | tee -a ${LOGFILE}
echo "${SIG_CMD} ${SELF_MD5SUM}" | tee -a ${LOGFILE}
echo "${SIG_CMD} ${LOGFILE_SIG_MD5SUM}" | tee -a ${LOGFILE}
echo "Signature file length             : ${LOGFILE_SIG_LEN} (${LOGFILE_SIG})"
echo "Current directory                 : `pwd`"
cat ${LOGFILE} | grep -oE "OPTION .+$" | tee -a ${LOGFILE}
cat ${LOGFILE} | grep -oE "[0-9]+ \[pub/s\]$" | \
awk '{
    if($1 != 0){
        x[NR] = $1
    }
}
END {
    if(NR == 0) {
        print "Average                           : --- [pub/sec] [n=0]"
        print "Variance                          : ---"
        print "Standard deviation                : ---"
        exit
    }
    for(i in x){
        sum_x += x[i]
    }
    m_x = sum_x / NR
    sum_dx2 = 0
    for(i in x){
        sum_dx2 += (x[i] - m_x) ^ 2
    }

    print "Sum                               : " sum_x " [pub] [n=" NR "]"
    print "Average                           : " m_x " [pub/sec] [n=" NR "]"
    print "Variance                          : " sum_dx2 / NR
    print "Standard deviation                : " sqrt(sum_dx2 / NR)
}' | tee -a ${LOGFILE}
echo "MQTT Publish error num            : `cat ${LOGFILE} | grep 'MQTT Publish error' | wc -l`" | tee -a ${LOGFILE}
echo "MQTT Connect error num            : `cat ${LOGFILE} | grep 'MQTT Connect error' | wc -l`" | tee -a ${LOGFILE}

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
