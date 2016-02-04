DATE=$(date +%Y%m%d -d "-1 days")
log_file=log/spo_import_wise.log


python main.py spo_import wise ${DATE} 1>>${log_file} 2>&1
