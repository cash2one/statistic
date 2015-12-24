DATE=$(date +%Y%m%d -d "-1 days")
log_file=log.${DATE}

echo ${DATE}
echo ${file_name}
echo ${output}

mkdir ${DATE}
wget ftp://nj02-wd-kg14.nj02.baidu.com/home/work/seagull/online_statistics/original_log/st01-kgb-haiou1.st01/access_${DATE}.log -O ${DATE}/st01-kgb-haiou1.st01
wget ftp://nj02-wd-kg14.nj02.baidu.com/home/work/seagull/online_statistics/original_log/st01-kgb-haiou2.st01/access_${DATE}.log -O ${DATE}/st01-kgb-haiou2.st01
wget ftp://nj02-wd-kg14.nj02.baidu.com/home/work/seagull/online_statistics/original_log/nj02-kgb-haiou1.nj02/access_${DATE}.log -O ${DATE}/nj02-kgb-haiou1.nj02
wget ftp://nj02-wd-kg14.nj02.baidu.com/home/work/seagull/online_statistics/original_log/nj02-kgb-haiou2.nj02/access_${DATE}.log -O ${DATE}/nj02-kgb-haiou2.nj02

echo ${DATE} >> err
cat ${DATE}/* | python parse_log.py 2>>err | sort | python merge.py > ${output}
