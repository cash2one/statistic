DATE=$(date +%Y%m%d -d "-3 days")
log_file=log/midpage_import_1.log


python main.py midpage_import 1 ${DATE} 1>>${log_file} 2>&1
