# -*- coding: utf-8 -*-

import datetime
try:
  import matplotlib.pyplot as plt
except ImportError:
  plt = None

def perf_client_attrs():
    prepare_data = '''sh: ssh $usr@$ip "${client_env_vars} client_idx=$idx $dir/$client/stress.sh prepare ${type} ${client_start_args}"'''
    return locals()

def perf_ct_attrs():
    prepare_data = 'all: client prepare_data type=all'
    reboot_to_prepare_data = 'seq: stop conf rsync clear prepare_data'
    return locals()

def perf_role_attrs():
    save_profile = '''sh: scp $usr@$ip:$dir/log/$role.profile $_rest_'''
    save_gprofile = '''sh: scp $usr@$ip:$gprofiler_output $_rest_'''
    return locals()

def perf_obi_attrs():
    perf_load_params = r'''call: obmysql extra="-e \"alter system set merge_delay_interval='1s' server_type=chunkserver;\""'''
    perf_create_tables = r'''call: obmysql extra="< perf/${case}.create"'''
    perf_init = 'seq: perf_load_params perf_create_tables'
    perf = 'seq: perf_preboot reboot sleep[sleep_time=2] perf_init perf_prepare.reboot_to_prepare_data turn_on_perf perf_run.reboot sleep[sleep_time=$ptime] perf_run.stop turn_off_perf collect_perf'
    perf_ups = 'seq: perf_preboot reboot sleep[sleep_time=2] perf_init turn_on_perf perf_run.reboot sleep[sleep_time=$ptime] perf_run.stop turn_off_perf collect_perf'
    collect_perf = 'seq: perf_preboot collect_perf_'
    running_ts = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    local_tmp_dir = "tmp.%s" % (running_ts)

    def perf_preboot(*args, **ob):
      ob['ms0']['server_ld_preload'] = "$dir/lib/libprofiler_helper.so"
      ob['ms0']['gprofiler_output'] = "$dir/ms0.gprofiler.output"
      ob['ms0']['environ_extras'] = "PROFILEOUTPUT=$gprofiler_output"
      ob['cs0']['server_ld_preload'] = "$dir/lib/libprofiler_helper.so"
      ob['cs0']['gprofiler_output'] = "$dir/cs0.gprofiler.output"
      ob['cs0']['environ_extras'] = "PROFILEOUTPUT=$gprofiler_output"

    def turn_on_perf(*args, **ob):
      # turn on profile log and gprofiler
      call_(ob, 'ms0.kill', '-46')
      call_(ob, 'ms0.kill', '-60')
      #call_(ob, 'cs0.kill', '-46')
      call_(ob, 'cs0.kill', '-60')

    def turn_off_perf(*args, **ob):
      # turn off profile log and gprofiler
      call_(ob, 'ms0.kill', '-45')
      call_(ob, 'ms0.kill', '-61')
      #call_(ob, 'cs0.kill', '-45')
      call_(ob, 'cs0.kill', '-61')

    def collect_perf_(*args, **ob):
      def pprof_profile_output(server_bin, profile_output, cost_graph_name, *args, **ob):
        pro_res = popen(sub2("pprof --text $server_bin $profile_output", locals())).splitlines()
        i = 0
        while i < len(pro_res):
          if pro_res[i].startswith('Total: '):
            i += 1
            break
          i += 1
        func_top50 = '\n'.join(pro_res[i:i + 50])
        sh(sub2("pprof --pdf $server_bin $profile_output > $cost_graph_name", locals()))
        return func_top50

      perf_result_dir = "/home/yanran.hfs/public_html/ob_perf/not_published/%s" % (running_ts)
      os.mkdir(perf_result_dir)
      os.mkdir(local_tmp_dir)
      call_(ob, 'ms0.save_profile', local_tmp_dir)
      call_(ob, 'ms0.save_gprofile', local_tmp_dir)
      call_(ob, 'cs0.save_profile', local_tmp_dir)
      call_(ob, 'cs0.save_gprofile', local_tmp_dir)
      
      ms_profile_output = "%s/%s" % (local_tmp_dir, str.split(find_attr(ob, "ms0.gprofiler_output"), '/')[-1])
      cs_profile_output = "%s/%s" % (local_tmp_dir, str.split(find_attr(ob, "cs0.gprofiler_output"), '/')[-1])
      ms_func_top50 = pprof_profile_output("bin/mergeserver", ms_profile_output,
                                           sub2("$perf_result_dir/ms_cost_graph.pdf", locals()))
      cs_func_top50 = pprof_profile_output("bin/chunkserver", cs_profile_output,
                                           sub2("$perf_result_dir/cs_cost_graph.pdf", locals()))

      time_format = "%Y-%m-%d %H:%M:%S"
      d = dict()
      start_time = None
      with open(sub2("$local_tmp_dir/mergeserver.profile", ob)) as f:
        for l in f:
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
          if qps2time.has_key(stat['ts']):
            qps2time[stat['ts']] += 1
          else:
            qps2time[stat['ts']] = 0
      qps = sql_count / elapsed_seconds
      avg_total_time = float(total_time) / sql_count
      avg_rpc_time = float(rpc_time) / sql_count
      avg_urpc_time = float(urpc_time) / sql_count
      avg_wait_time = float(wait_time) / sql_count
      if plt is not None:
        plt.plot(qps2time.values(), '-')
        plt.xlabel('Timeline')
        plt.ylabel('QPS')
        plt.savefig("%s/qps.png" % (perf_result_dir))
        plt.clf()
        plt.bar(total_times_dist.keys(), total_times_dist.values())
        plt.xlabel('Response Time (us)')
        plt.ylabel('Number of Requests')
        plt.savefig("%s/total_time_dist.png" % (perf_result_dir))
        plt.clf()
        plt.plot(rpc_times, ',')
        plt.xlabel('Timeline')
        plt.ylabel('Response Time (us)')
        plt.savefig("%s/csups_rt.png" % (perf_result_dir))
        plt.clf()
        plt.plot(urpc_times, ',')
        plt.xlabel('Timeline')
        plt.ylabel('Response Time (us)')
        plt.savefig("%s/ups_rt.png" % (perf_result_dir))
        plt.clf()
        plt.plot(wait_times, ',')
        plt.xlabel('Timeline')
        plt.ylabel('Wait Time in Mergeserver Queue (us)')
        plt.savefig("%s/queue_time.png" % (perf_result_dir))
        plt.clf()
      user_name = find_attr(ob, "usr")
      case_name = find_attr(ob, "case")
      result_html_template = ("""
<p>测试人员：${user_name}<br>
测试Case：${case_name}<br>
测试运行时间：${elapsed_seconds}s<br>
SQL请求次数：$sql_count<br>
MS的QPS：$qps</p>
<p>QPS：<br>
<img src="qps.png" /></p>
<p>平均响应延时：${avg_total_time}us<br>
<img src="total_time_dist.png" /></p>
<p>CS&UPS平均响应延时：${avg_rpc_time}us<br>
<img src="csups_rt.png" /></p>
<p>UPS修改平均响应延时：${avg_urpc_time}us<br>
<img src="ups_rt.png" /></p>
<p>MS队列中平均花费的时间：${avg_wait_time}us<br>
<img src="queue_time.png" /></p>
<p>MS函数消耗排名前50："
<pre>${ms_func_top50}</pre></p>"
<p><a href="ms_cost_graph.pdf">函数消耗线框图</a></p>
<p>CS函数消耗排名前50："
<pre>${cs_func_top50}</pre></p>"
<p><a href="cs_cost_graph.pdf">函数消耗线框图</a></p>
""")
      with open("%s/index.html" % (perf_result_dir), "w") as f:
        f.write(sub2(result_html_template, locals()))
      with open("/home/yanran.hfs/public_html/ob_perf/not_published/index.html", "a") as f:
        f.write("""<li><a href="%s/">%s %s %s</a></li>\n""" % (running_ts, running_ts, user_name, case_name))
      sh("rm -r %s" % (local_tmp_dir))

    return locals()

def perf_install():
    client_vars.update(dict_filter_out_special_attrs(perf_client_attrs()))
    ct_vars.update(dict_filter_out_special_attrs(perf_ct_attrs()))
    role_vars.update(dict_filter_out_special_attrs(perf_role_attrs()))
    obi_vars.update(dict_filter_out_special_attrs(perf_obi_attrs()))

perf_install()

