# MetricViewer

## Oracle

Copy files, edit application.conf, put ojdbc7.jar in the lib directory, start with java as following (replace ``:`` with ``;`` on windows):

``java -Dconfig.file=application.conf -Xmx2G -Xms1G -cp "lib/ojdbc7.jar:lib/backend.jar" com.pythian.webui.Boot``

Open in browser http://host:port. Click on DB sign right upper corner, enter query you want to visualize. 
Query must satisfy the following rules:
  * select list is \<timestamp|date\>, \<metric name:varchar2\>, \<value:number\>
  * result has to be ordered by first (time) column
  
Notes:
  * data step is fixed to 10 seconds intervals
  * data came to the same interval is aggregated as sum
  * at larger scales data is shown as ``sum(value) / scale``
  * keep metric set as small as possible, having all SQL_IDs (instead lets say top N + other) on the graph will kill performance
  * example query1:
```sql
select sample_time, decode(session_state, 'WAITING', wait_class, 'CPU'), count(*) 
from dba_hist_active_sess_history
group by sample_time, decode(session_state, 'WAITING', wait_class, 'CPU')
order by sample_time
```
  * example query2:
```sql
select 
    systimestamp - numToDSInterval( 10 * trunc(level / 3), 'second' ), 
    decode(mod(level, 3), 0, 'mod(level, 360)', 1, 'sin', 2, '2 * mod(level, 360/5)'),
    decode(mod(level, 3), 0, mod(level, 360), 1, 1500 + 1500 * sin(level / 1000), 2, 2 * mod(level, 360/5)) 
from dual connect by level <= 100000
order by 1
```