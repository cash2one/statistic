DATE=$(date +%Y%m%d -d "-4 days")
log_file=log/spo_import_kpi.log


python main.py spo_kpi_import ${DATE} 1>>${log_file} 2>&1
