#samples: vip_addr=10.232.23.16:19053:bond0
addr=$1
rs_vip=`echo $addr|awk -F: '{print $1}'`
rs_port=`echo $addr|awk -F: '{print $2}'`
nic=`echo $addr|awk -F: '{print $3}'`
base_dir=$2
user=$3

echo "vip=$rs_vip, rs_port:$rs_port, nic=$nic, base_dir=$base_dir, user=$user"

cp -f rs_tpl.xml rs.xml
sed -i "s:VIP:${rs_vip}:; s:NIC:${nic}:; s:BASEDIR:${base_dir}:; s:RSPORT:${rs_port}:; s:USER:${user}:" rs.xml
cp -f RootServer /usr/lib/ocf/resource.d/heartbeat/
chmod +x /usr/lib/ocf/resource.d/heartbeat/RootServer
cp -f ob_ping $base_dir/bin/
chown ${user} $base_dir/bin/ob_ping
chmod +x $base_dir/bin/ob_ping
crm_attribute --type crm_config --attr-name symmetric-cluster --attr-value true
crm_attribute --type crm_config --attr-name stonith-enabled --attr-value false
crm_attribute --type rsc_defaults --name resource-stickiness --update 100
cibadmin --replace --obj_type=resources --xml-file ./rs.xml
