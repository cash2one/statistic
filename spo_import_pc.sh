DATE=$(date +%Y%m%d -d "-4 days")
log_file=log/spo_import_pc.log


python main.py spo_import pc ${DATE} 1>>${log_file} 2>&1
