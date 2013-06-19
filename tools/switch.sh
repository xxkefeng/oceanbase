#!/usr/bin/env sh

RS_PORT=2500
RS_ADMIN="oceanbase/bin/rs_admin"

LOG=eval' ''echo line $LINENO:'

t=`getopt -o m:s:h:z: -- "$@"`

eval set -- "$t"

while true; do
  case $1 in
    "-m")
    MASTER=$2
    ;;  
    "-s")
    SLAVE=$2
    ;;  
    "-h")
    MYSQL_HOST=$2
    ;;
    "-z")
    MYSQL_PORT=$2
    ;;
    "--")
    break
    ;;  
  esac
  shift
done

function usage()
{
  echo "usage:"
  echo "./switch.sh -m master_ip -s slave_ip -h mysql_host_ip -z mysql_port"
}

if [ ! $MASTER ]; then
  usage
  exit 1
fi
if [ ! $SLAVE ]; then
  usage
  exit 1
fi
if [ ! $MYSQL_HOST ]; then
  usage
  exit 1
fi
if [ ! $MYSQL_PORT ]; then
  usage
  exit 1
fi

MYSQL_CMD="mysql -h $MYSQL_HOST -P$MYSQL_PORT -u admin -padmin"

#检查集群的状态，确认是否能做切换
$RS_ADMIN -r $MASTER -p $RS_PORT get_obi_role 2>&1 | grep "obi_role=MASTER"
if [ $? -ne 0 ]; then
  $LOG "fail to get obi role or not master" $MASTER
  exit 1
fi

$RS_ADMIN -r $SLAVE -p $RS_PORT get_obi_role 2>&1 | grep "obi_role=SLAVE"
if [ $? -ne 0 ]; then
  $LOG "fail to get obi role or not slave" $SLAVE
  exit 1
fi

TMP=$($MYSQL_CMD -Bse "show parameters like 'master_root_server_ip';" | awk -F\\t '{print $6}' | uniq)
if [ "$MASTER" != "$TMP" ]; then
  $LOG "fail to alter master root ip"
  exit 1
fi

TMP=$($MYSQL_CMD -Bse "show parameters like 'master_root_server_port';" | awk -F\\t '{print $6}' | uniq)
if [ "$RS_PORT" != "$TMP" ]; then
  $LOG "fail to alter master root port"
  exit 1
fi

$MYSQL_CMD -Bse "select cluster_role from __all_cluster where cluster_vip = '$MASTER';" | grep "1"
if [ $? -ne 0 ]; then
  $LOG "master should be 1 now"
  exit 1
fi

$MYSQL_CMD -Bse "select cluster_role from __all_cluster where cluster_vip = '$SLAVE';" | grep "2"
if [ $? -ne 0 ]; then
  $LOG "slave should be 2 now"
  exit 1
fi

echo "是否执行主被切换[y/N]"
read answer 
if [ $answer != 'y' ]; then
  exit 1
fi


#把主设置为被集群
$RS_ADMIN -r $MASTER -p $RS_PORT get_obi_role 2>&1 | grep "obi_role=MASTER"
if [ $? -ne 0 ]; then
  $LOG "fail to get obi role or not master" $MASTER
  exit 1
fi

$RS_ADMIN -r $MASTER -p $RS_PORT set_obi_role -o OBI_SLAVE
if [ $? -ne 0 ]; then
  $LOG "fail to set obi to slave" $MASTER
  exit 1
fi

$RS_ADMIN -r $MASTER -p $RS_PORT get_obi_role 2>&1 | grep "obi_role=SLAVE"
if [ $? -ne 0 ]; then
  $LOG "fail to set obi to slave" $MASTER
  exit 1
fi

$LOG "sleep 1s wait for master to slave"
sleep 1

#把被设置为主
$RS_ADMIN -r $SLAVE -p $RS_PORT get_obi_role 2>&1 | grep "obi_role=SLAVE"
if [ $? -ne 0 ]; then
  $LOG "fail to get obi role or not slave" $SLAVE
  exit 1
fi

$RS_ADMIN -r $SLAVE -p $RS_PORT set_obi_role -o OBI_MASTER 
if [ $? -ne 0 ]; then
  $LOG "fail to set obi to master" $SLAVE
  exit 1
fi

$RS_ADMIN -r $SLAVE -p $RS_PORT get_obi_role 2>&1 | grep "obi_role=MASTER"
if [ $? -ne 0 ]; then
  $LOG "fail to set obi to slave" $MASTER
  exit 1
fi

#交换ip地址
TMP=$MASTER
MASTER=$SLAVE
SLAVE=$TMP

#更改所有集群记录的主集群地址
$RS_ADMIN -r $MASTER -p $RS_PORT set_config -o master_root_server_ip=$MASTER,master_root_server_port=$RS_PORT
if [ $? -ne 0 ]; then
  $LOG "fail to set master address" $MASTER
  exit 1
fi
$RS_ADMIN -r $SLAVE -p $RS_PORT set_config -o master_root_server_ip=$MASTER,master_root_server_port=$RS_PORT
if [ $? -ne 0 ]; then
  $LOG "fail to set master address" $SLAVE
  exit 1
fi

$LOG "sleep 5s"
sleep 5

#用mysql客户端连接集群，设置主集群信息
$MYSQL_CMD -Bse "alter system set master_root_server_ip='$MASTER' server_type=rootserver, master_root_server_port=$RS_PORT server_type=rootserver;"
if [ $? -ne 0 ]; then
  $LOG "fail to alter master root ip in mysql"
  exit 1
fi

$LOG "sleep 15s wait for alter system change"
sleep 15

TMP=$($MYSQL_CMD -Bse "show parameters like 'master_root_server_ip';" | awk -F\\t '{print $6}' | uniq)
if [ "$MASTER" != "$TMP" ]; then
  $LOG "fail to alter master root ip"
  exit 1
fi

TMP=$($MYSQL_CMD -Bse "show parameters like 'master_root_server_port';" | awk -F\\t '{print $6}' | uniq)
if [ "$RS_PORT" != "$TMP" ]; then
  $LOG "fail to alter master root port"
  exit 1
fi

#修改cluster_role
$MYSQL_CMD -Bse "select cluster_role from __all_cluster where cluster_vip = '$MASTER';" | grep "2"
if [ $? -ne 0 ]; then
  $LOG "master should be 2 now"
  exit 1
fi

$MYSQL_CMD -Bse "select cluster_role from __all_cluster where cluster_vip = '$SLAVE';" | grep "1"
if [ $? -ne 0 ]; then
  $LOG "slave should be 1 now"
  exit 1
fi

MASTER_CLUSTER_ID=$($MYSQL_CMD -Bse "select cluster_id from __all_cluster where cluster_vip = '$MASTER';")
if [ ! $MASTER_CLUSTER_ID ]; then
  $LOG "fail to get master cluster id"
  exit 1
fi

SLAVE_CLUSTER_ID=$($MYSQL_CMD -Bse "select cluster_id from __all_cluster where cluster_vip = '$SLAVE';")
if [ ! $SLAVE_CLUSTER_ID ]; then
  $LOG "fail to get slave cluster id"
  exit 1
fi

$MYSQL_CMD -Bse "update __all_cluster set cluster_role=1 where cluster_id=$MASTER_CLUSTER_ID;"
if [ $? -ne 0 ]; then
  $LOG "fail to set master cluster role"
  exit 1
fi

$MYSQL_CMD -Bse "update __all_cluster set cluster_role=2 where cluster_id=$SLAVE_CLUSTER_ID;"
if [ $? -ne 0 ]; then
  $LOG "fail to set slave cluster role"
  exit 1
fi

$MYSQL_CMD -Bse "select cluster_role from __all_cluster where cluster_vip = '$MASTER';" | grep "1"
if [ $? -ne 0 ]; then
  $LOG "master should be 1 now"
  exit 1
fi

$MYSQL_CMD -Bse "select cluster_role from __all_cluster where cluster_vip = '$SLAVE';" | grep "2"
if [ $? -ne 0 ]; then
  $LOG "slave should be 2 now"
  exit 1
fi

$LOG "switch success"

