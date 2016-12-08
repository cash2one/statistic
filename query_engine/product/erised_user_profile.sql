-- 从erised_user_profile中分类汇总查询数据

SELECT event_query, count(event_query) as query_count
FROM udwetl_psquery
WHERE event_day='${hivevar:day}'    -- There is a substitution variable that can be passed from command line.
GROUP BY event_query
ORDER BY query_count DESC
LIMIT 50;

set hive.mapred.mode=nonstrict;
sessiondb = DATABASE 'session:/';
USE sessiondb;
CREATE TABLE tmp_tbl
(event_mac string)
ROW FORMAT
DELIMITED
FIELDS TERMINATED BY ' ';
add file ./macids.txt;
LOAD DATA LOCAL INPATH './macids.txt' overwrite INTO TABLE tmp_tbl;
select e.*
from (
    select id,*
    from default.erised_user_profile_all
    lateral view explode(otherid) tmpTable as id
    where event_day=20141118 and id like 'mac_%'
) e join tmp_tbl d on concat("mac_", d.event_mac) == e.id