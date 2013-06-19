def get_extract_pat(target):
    ''' target example: 'master_write.tab' '''
    rules_ = """
master_write: master_end_trans: log_id=\[${start_id:int:\d+},${end_id:int:\d+}\], time=\[${start_time:int:\d+},${end_time:int:\d+}\]\[${write_time:int:\d+}:${store_time:int:\d+}:${send_time:int:\d+}\]
slave_receive: slave_receive_log\(cur_ts=${start_receive_time:int:\d+}, net_time=\[${send_ts:int:\d+},${receive_ts:int:\d+}\], log=\[${start_id:int:\d+},${end_id:int:\d+}\]\)
slave_replay: slave_end_replay\(log_id=\[${start_id:int:\d+},${end_id:int:\d+}\], time=\[${start_replay_time:int:\d+},${end_replay_time:int:\d+}\]\[${replay_time:int:\d+}:${store_time:int:\d+}:${apply_time:int:\d+}\]
flush: f=\[flush_log\] flush_start=${start:int:\d+} \| f=\[flush_log\] write_log disk=${disk:int:\d+} net=${net:int:\d+}, log=${start_id:int:\d+}:${end_id:int:\d+}.*total_timeu=${time:int:\d+}
apply: f=\[handle_write_trans_\].*total_timeu=${time:int:\d+}
dml: DML total=\d+, TPS=${tps:int:\d+}
client: write=\[total=\d+:\d+\]\[tps=${tps:int:\d+}:\d+\]\[thread=\d+:\d+\]\[rt=${rt:int:\d+}:\d+\],.*, mget=\[total=\d+:\d+\]\[qps=${qps:int:\d+}:\d+\]\[thread=\d+:\d+\]\[rt=${qrt:int:\d+}:\d+\]
"""
    rules = dict(re.findall('^\s*(\w+): (.+)\s*$', rules_, re.M))
    m = re.search('(\w+).tab$', target)
    if not m: raise Fail('not recognized tab_file[%s]'%(target))
    ex_pat = rules.get(m.group(1), None)
    if not ex_pat: raise Fail('not supported tab_file[%s]'%(target))
    return ex_pat

def profile_role_attrs(ob):
    perf_collect = '''sh: mkdir -p $plog_dir && rsync -avz $usr@$ip:$dir/log/$role.perf.$ip:$port $plog_dir/'''
    perf_report = '''sh: pprof --lib_prefix=./lib --$report_type bin$ver/$role "$plog_dir/$role.perf.$ip:$port" >perf.$report_type'''
    report_type = 'text'
    def extract2(**ups):
        cmd = """b/findall.py '%s' <$collected_log_dir/$run_id/$role.log.$ip:$port > $target_tab_file"""
        pat = get_extract_pat(find_attr(ups, 'target_tab_file'))
        expand_cmd = sub2(cmd, ups) %(pat)
        print expand_cmd
        return sh(expand_cmd)
    return locals()

def list_sub(la, lb):
    for x in lb:
        try:
            la.remove(x)
        except ValueError as e:
            pass
    return la
def profile_obi_attrs(ob):
    stop_servers = 'par: .+server kill_by_name'
    stop = 'seq: stop_servers ups0.kill_lz4c'
    lz4c_cmd = '$dir/tools$tool_ver/lz4c'
    init_sql_file = 'b/init.sql'
    def extract_replay_latency_stat(**ob):
        ups_list = get_match_child(ob, 'updateserver').keys()
        if len(ups_list) != 2: raise Fail('only two UPS supported!')
        ups_master = call_(ob, 'get_master_ups_name')
        ups_slave = list_sub(ups_list, [ups_master])[0]
        master_write = call(ob, '%s.extract2'%(ups_master), target_tab_file='master_write.tab')
        slave_receive = call(ob, '%s.extract2'%(ups_slave), target_tab_file='slave_receive.tab')
        slave_replay = call(ob, '%s.extract2'%(ups_slave), target_tab_file='slave_replay.tab')
        return (('collect_log', collect_log),
                    ('master_write', master_write), ('slave_receive', slave_receive), ('slave_replay', slave_replay))
    plog_dir = '$collected_log_dir/$run_id'
    mark_log_collected = 'sh: touch $plog_dir/collect_log.done'
    is_log_collected = 'sh: [ -e $plog_dir/collect_log.done ]'
    mark_profile_done = 'sh: touch $plog_dir/profile.done'
    is_profile_done = 'sh: [ -e $plog_dir/profile.done ]'
    extract_profile_data = '''seq: ct.c0.extract2[target_tab_file=$plog_dir/client.tab]
 ups0.extract2[target_tab_file=$plog_dir/dml.tab]'''
    plot_client = '''sh: b/tquery.py '$plog_dir/$$name.tab' 'select plot("$plog_dir/tps.png,r,", tps), plot("$plog_dir/rt.png,r,", rt), plot("$plog_dir/qps.png,r,", qps), plot("$plog_dir/qrt.png,r,", qrt) from t_client' '''
    plot_dml = '''sh: b/tquery.py '$plog_dir/$$name.tab' 'select plot("$plog_dir/dml.png,r,", tps) from t_dml' '''
    plot_flush = '''#sh: b/tquery.py '$plog_dir/$$name.tab' 'select plot("$plog_dir/batch.png,r,", end_id-start_id), plot("$plog_dir/net.png,b+", net), plot("$plog_dir/disk.png,b+", disk) from t_flush' '''
    profile_sleep = 'sh: sleep $profile_duration'
    create_table = '''xsh: mysql -h ${ms0.ip} -P ${ms0.mysql_port} -uadmin -padmin -e "\. $init_sql_file" '''
    collect_profile_log = 'seq: collect_server_log ct.c0.collect_log'
    turn_on_trace_log = '#call: ups0.kill[-41]'
    get_profile_log = 'seq: reboot create_table turn_on_trace_log ct.reboot profile_sleep collect_server_log ct.c0.collect_log'
    do_profile = 'seq: require_profile_log extract_profile_data plot profile_cleanup'
    plot = 'seq: plot_client plot_dml'
    profile_cleanup = 'seq: ct.stop stop ct.force_stop force_stop'
    profile_cleanup_data = 'seq: ct.clear cleanup'
    def require_profile_log(**ob):
        if call_(ob, 'is_log_collected') == 0:
            return 'Aready Collected'
        return call(ob, 'get_profile_log'), call(ob, 'mark_log_collected')
    def profile(**ob):
        if call_(ob, 'is_profile_done') == 0:
            return 'Aready Done'
        return call(ob, 'do_profile'), call(ob, 'mark_profile_done')
    def safe_profile(**ob):
        ret = 'OK'
        try:
            return call_(ob, 'profile')
        except Exception,e:
            ret = 'FAIL'
            print 'Profile Fail, %s'%(e)
        if find_attr(ob, 'cleanup_data_after_profile'):
            print call_(ob, 'profile_cleanup_data');
        return ret
    return locals()

def ProfilerAttr():
    role = 'profiler'
    report_file = 'report.html'
    run = 'all: obi safe_profile'
    cleanup = 'all: obi profile_cleanup'
    cleanup_data = 'all: obi profile_cleanup_data'
    collect_log = 'all: obi collect_profile_log'
    extract_profile_data = 'all: obi extract_profile_data'
    plot = 'all: obi plot'
    def gen_test_meta(**attr):
        path, meta = sub2('${collected_log_dir}/meta', attr), sub2('$test_meta', attr)
        write(path, meta)
        return path
    do_report = 'sh: b/report.py ${collected_log_dir} ${collected_log_dir}/${report_file} "$profile_spec"'
    profile = 'seq: run cleanup report'
    report = 'seq: gen_test_meta do_report'
    test_meta = '''server: $server_hosts
clients: $client_hosts
duration: $profile_duration s
server_threads: $server_threads'''
    def update_local_bin(**cfg):
        obi_list = get_match_child(cfg, 'obi').values()
        if not obi_list: return 'no obi defined!'
        return call(obi_list[0], 'update_local_bin')
    id = 'all: obi id'
    return locals()
def Profiler(**cfg):
    [v.get('name') or v.update(name=k) for k,v in get_match_child(cfg, 'obi').items()]
    return dict_updated(dict_filter_out_special_attrs(ProfilerAttr()), **cfg)

def client_attrs(__self__):
    def extract2(**client):
        cmd = """b/findall.py '%s' <$collected_log_dir/$run_id/$client_log > $target_tab_file"""
        pat = get_extract_pat(find_attr(client, 'target_tab_file'))
        return sh(sub2(cmd, client) %(pat))
    return locals()

def monitor_install():
    client_vars.update(dict_filter_out_special_attrs(client_attrs(client_vars)))
    role_vars.update(dict_filter_out_special_attrs(profile_role_attrs(role_vars)))
    ObCfg.__dict__.update(Profiler=Profiler)
    def set_profiler_name(cfg):
        [v.get('profiler_name') or v.update(profiler_name=k) for k,v in get_match_child(cfg, 'profiler').items()]
    ObCfg.after_load_hook.append(set_profiler_name)
    ObCfg.server_start_environ += '  GPERF_FILE=$dir/log/$role.perf.$ip:$port pcap_cmd="$pcap_cmd" pfetch_cmd="$pfetch_cmd"'
    ObCfg.local_tools += ',lz4c'
    obi_vars.update(dict_filter_out_special_attrs(profile_obi_attrs(obi_vars)))
monitor_install()
