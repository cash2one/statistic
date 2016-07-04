date=20160619
for((date=20160619;date>20160610;date--));do
    echo $date
    python main.py midpage_statist $date
done
