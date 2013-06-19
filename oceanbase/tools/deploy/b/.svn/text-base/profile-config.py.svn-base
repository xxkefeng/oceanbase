## comment line below if you need to run './deploy.py ob1.random_test ...'
load_file('monitor.py', 'fault_test.py')
#data_dir = '/home/yuanqi.xhf/data'         # $data_dir/{1..10} should exist
data_dir = '/data/'
## comment line below if you want to provide custom schema and server conf template(see ob5's definition for another way)
## run: './deploy.py tpl.gensvrcfg' to generate a 'tpl' dir, edit tpl/rootserver.template... as you wish 
tpl = load_dir_vars('tpl.4x2')  # template of config file such as rootserver.conf/updateserver.conf...

ObCfg.usr = 'admin'
ObCfg.home = '/home/admin'
data_dir='/data'
sub_dir_list='{1..6}'
dev='bond0'
total_memory_limit = 140
table_memory_limit = 128
log_sync_type = 1
trace_log_level = 'debug'
clog_dir = '/data/log1'

load_file('b/profile.py', 'b/extra.py')
init_sql_file='b/init.sql'
collected_log_dir = 'L/%s.$profiler_name' %(popen('date +%y%j').strip())
def make_profiler(server_hosts, client_hosts, profile_spec):
    def make_profile_obi(spec, hosts, clients):
        def expand_host(spec):
            return list_merge(map(multiple_expand, spec.split()))
        online_common_attr = dict(
            exception_on_bootstrap_fail=1,
            rs_admin_cmd='ssh $gateway tools$tool_ver/rs_admin',
            ups_admin_cmd='ssh $gateway tools$tool_ver/ups_admin',
            client_cmd='ssh $gateway tools$tool_ver/client',
            gateway='login1.cm4.taobao.org',
            need_lsync=False, new_style_config='True', tpl=load_dir_vars('tpl.4x2'),
            hosts=hosts, replay_checksum_flag = 1,
            clog_src='~/log.dev', wait_slave_sync_time_us=100004, boot_strap_timeout=60000000, profile_duration='600',
            )
        pat = 'c(\d+)x(\d+)_([a-z]+)(\d+)_(.+)_(.+)_(.+)'
        m = re.match(pat, spec)
        if not m: raise Fail('Not Valid Obi Spec for Profile', spec)
        client_num, thread_num, write_type, trans_size, dist, table_name, ver = m.groups()
        client_num, thread_num, trans_size = int(client_num), int(thread_num), int(trans_size)
        if write_type == 'muget':
            write_type = 'mutator'
            mget_thread_num = thread_num
        else:
            mget_thread_num = 0
        return OBI(ct=CT('simple_client.dev', hosts=clients[:client_num], n_transport=4, table=table_name,
                         write=thread_num, write_type=write_type, dist=dist, write_size=trans_size,
                         mget=mget_thread_num, mget_size=trans_size), run_id=spec,
                   src='~/ob.%s'%(ver), ver='.%s'%(ver), tool_ver='.dev',
                   **online_common_attr)
    profiler_common_attr = dict(server_hosts=' '.join(server_hosts),
                                client_hosts=' '.join(client_hosts),
                                server_threads=22,
                                profile_spec=' '.join(profile_spec), profile_duration='600')
    profiler = Profiler(report_file='report.html', **dict([(spec, make_profile_obi(spec, server_hosts, client_hosts)) for spec in profile_spec]))
    return dict_updated(profiler_common_attr, **profiler)
online_hosts = expand('172.24.131.[194,193]')
online_clients = expand('10.246.164.[31,32,33,34,35,37,38,39,40]')
profile_spec = ' '.join(expand('c01x001_[muget,mutator,phyplan]01_urand_i01xi01_dev c08x010_[mget,mutator,phyplan]01_urand_mix_dev c08x010_mutator01_fixed_mixed_dev'))
p0 = make_profiler(online_hosts[1:2], online_clients, expand('c08x100_mutator20_urand_i01xi01_dev-cq'))
p1 = make_profiler(online_hosts[1:2], online_clients, expand('c08x[010,100]_muget01_urand_mix_dev c08x100_muget20_urand_mix_dev'))

print '\n'.join(online_clients)
