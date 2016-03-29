if [ $# -lt 2 ]; then
    echo "USAGE: $0 startdate enddate substring"
    exit 0
fi
start=` date "+%s" -d $1`
end=` date "+%s" -d $2`
today=` date "+%s"`
(( sdiff=($today - $start)/86400 ))
(( ediff=($today - $end)/86400 ))

(( diff=($end - $start)/86400 ))
for((i=$sdiff; i >= $ediff; i -= 1))
do
    day=`date -d "$i days ago" "+%Y%m%d"` 
    echo $day
    python main.py spo_import wise $day
    python main.py spo_import pc $day 
    python main.py spo_kpi_import $day
done
