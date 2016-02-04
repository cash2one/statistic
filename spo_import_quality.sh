DATE=$(date +%Y%m%d -d "-1 days")
log_file=log/spo_import_quality.log


python main.py spo_quality_import ${DATE} 1>>${log_file} 2>&1
