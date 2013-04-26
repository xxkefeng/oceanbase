import datetime
import re
import sys
try:
  import matplotlib.pyplot as plt
except ImportError:
  plt = None

time_format = "%Y-%m-%d %H:%M:%S"
d = dict()
start_time = None
for l in sys.stdin:
  if l.startswith('['):
    end_time = l[1:20]
    if start_time is None:
      start_time = end_time
  m = re.search(r'trace_id=\[(.*)\] source_chid=\[.*\] chid=\[.*\] sql=\[.*\] wait_time_us_in_sql_queue=\[(.*)\] pcode=\[.*\]', l)
  if m is not None:
    trace_id, wait_time = m.group(1), int(m.group(2))
    if not d.has_key(trace_id):
      d[trace_id] = dict()
    d[trace_id]['wait_time'] = wait_time
  else:
    m = re.search(r'trace_id=\[(.*)\] source_chid=\[.*\] chid=\[.*\] async_rpc_start_time=\[(.*)\] self=\[.*\] peer=\[.*\] pcode=\[.*\] ret=\[.*\]', l)
    if m is not None:
      trace_id, rpc_st = m.group(1), int(m.group(2))
      if not d.has_key(trace_id):
        d[trace_id] = dict()
      d[trace_id]['rpc_st'] = rpc_st
    else:
      m = re.search(r'trace_id=\[.*\] source_chid=\[.*\] chid=\[.*\] trace_id=\[(.*)\] chid=\[.*\] async_rpc_end_time=\[(.*)\]', l)
      if m is not None:
        trace_id, rpc_et = m.group(1), int(m.group(2))
        if not d.has_key(trace_id):
          d[trace_id] = dict()
        d[trace_id]['rpc_et'] = rpc_et
      else:
        m = re.search(r'\[(.*)\] DEBUG.*trace_id=\[(.*)\] source_chid=\[.*\] chid=\[.*\] handle_sql_time_ms=\[(.*)\]', l)
        if m is not None:
          dts, trace_id, total_time = m.group(1), m.group(2), int(m.group(3))
          if not d.has_key(trace_id):
            d[trace_id] = dict()
          d[trace_id]['total_time'] = total_time * 1000
          d[trace_id]['ts'] = int(datetime.datetime.strptime(dts, time_format).strftime("%Y%m%d%H%M%S"))
        else:
          m = re.search(r'trace_id=\[(.*)\] source_chid=\[.*\] chid=\[.*\] sync_rpc_start_time=\[(.*)\] self=\[.*\] peer=\[.*\] pcode=\[.*\]', l)
          if m is not None:
            trace_id, urpc_st = m.group(1), int(m.group(2))
            if not d.has_key(trace_id):
              d[trace_id] = dict()
            d[trace_id]['urpc_st'] = urpc_st
          else:
            m = re.search(r'trace_id=\[(.*)\] source_chid=\[.*\] chid=\[.*\] sync_rpc_end_time=\[(.*)\] ret=\[.*\] pcode=\[.*\]', l)
            if m is not None:
              trace_id, urpc_et = m.group(1), int(m.group(2))
              if not d.has_key(trace_id):
                d[trace_id] = dict()
              d[trace_id]['urpc_et'] = urpc_et
td = (datetime.datetime.strptime(end_time, time_format)
                   - datetime.datetime.strptime(start_time, time_format))
elapsed_seconds = (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6
sql_count = 0
total_time = 0
rpc_time = 0
urpc_time = 0
wait_time = 0
total_times_dist = dict()
rpc_times = []
urpc_times = []
wait_times = []
qps2time = dict()
for trace_id in d:
  stat = d[trace_id]
  if not (stat.has_key('wait_time') and stat.has_key('total_time')
      and ((stat.has_key('rpc_st') and stat.has_key('rpc_et'))
      or (stat.has_key('urpc_st') and stat.has_key('urpc_et')))):
    pass
  else:
    if stat.has_key('rpc_st'):
      stat['rpc_time'] = stat['rpc_et'] - stat['rpc_st']
      rpc_times.append(stat['rpc_time'])
      rpc_time += stat['rpc_time']
    if stat.has_key('urpc_st'):
      stat['urpc_time'] = stat['urpc_et'] - stat['urpc_st']
      urpc_times.append(stat['urpc_time'])
      urpc_time += stat['urpc_time']
    if total_times_dist.has_key(stat['total_time']):
      total_times_dist[stat['total_time']] += 1
    else:
      total_times_dist[stat['total_time']] = 0
    wait_times.append(stat['wait_time'])
    total_time += stat['total_time']
    wait_time += stat['wait_time']
    sql_count += 1
    ts = stat['ts']
    if qps2time.has_key(ts):
      qps2time[ts] += 1
    else:
      qps2time[ts] = 0
qps = sql_count / elapsed_seconds
avg_total_time = float(total_time) / sql_count
avg_rpc_time = float(rpc_time) / sql_count
avg_urpc_time = float(urpc_time) / sql_count
avg_wait_time = float(wait_time) / sql_count
print "QPS: %d" % (qps)
print "AVG TIME: %f" % (avg_total_time)
print "AVG RPC TIME: %f" % (avg_rpc_time)
print "AVG WAIT TIME: %f" % (avg_wait_time)

