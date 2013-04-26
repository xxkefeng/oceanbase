#!/usr/bin/perl -w

# Author: 符风 <fufeng.syd@alipay.com>
#
# Todo:
# 1. remove dependence of oceanbase root dir.
# 2. promote precise of envrionment checking and command running.
# 3. ...

use Data::Dumper;
use Getopt::Long;
use Pod::Usage;
use vars qw($master_rs_vip $master_rs_port $global_cfg $init_cfg $action @clusters);
use strict;
use warnings;
use DBI();

sub for_hash {
  my ($hash, $fn) = @_;
  while (my ($key, $value) = each %$hash) {
    if ('HASH' eq ref $value) {
      for_hash($value, $fn);
    } else {
      $fn->($value);
    }
  }
}

sub check_ssh {
  system("ssh -T -o PreferredAuthentications=publickey $_[0] -l admin -o ConnectTimeout=1 true") == 0
    or die "ssh check failed! host: [$_[0]]";
  return $?;
}

sub delete_cluster_data ($) {
  my $cluster = shift;
  my $appname = $global_cfg->{public}{appname};

  my $cs = $cluster->{chunkserver};
  my $cs_max_disk_num = $global_cfg->{chunkserver}{max_disk_num};
  for_hash $cs, sub {
    my ($value) = @_;
    if ($value =~ /^\s*(\d+\.\d+\.\d+\.\d+)\s*$/) {
      my $cmd = "rm -rf /data/{1..$cs_max_disk_num}/$appname/ /home/admin/oceanbase/{log,data,run}";
      do_ssh($value, $cmd);
    }
  };

  my $ms = $cluster->{mergeserver};
  for_hash $ms, sub {
    my ($value) = @_;
    if ($value =~ /^\s*(\d+\.\d+\.\d+\.\d+)\s*$/) {
      my $cmd = "rm -rf /home/admin/oceanbase/{log,data,run}";
      do_ssh($value, $cmd);
    }
  };

  my $ups = $cluster->{updateserver};
  my $ups_commitlog_dir = $global_cfg->{updateserver}{commitlog_dir};
  my $ups_max_disk_num = $global_cfg->{updateserver}{max_disk_num};
  for_hash $ups, sub {
    my ($value) = @_;
    if ($value =~ /^\s*(\d+\.\d+\.\d+\.\d+)\s*$/) {
      my $cmd = "rm -rf /data/{1..$ups_max_disk_num}/$appname/ $ups_commitlog_dir /home/admin/oceanbase/{log,data,run}";
      do_ssh($value, $cmd);
    }
  };

  my $rs = $cluster->{rootserver};
  my $rs_commitlog_dir = $global_cfg->{rootserver}{commitlog_dir};
  for_hash $rs, sub {
    my ($value) = @_;
    if ($value =~ /^\s*(\d+\.\d+\.\d+\.\d+)\s*$/) {
      my $cmd = "rm -rf $rs_commitlog_dir /home/admin/oceanbase/{log,data,run}";
      do_ssh($value, $cmd);
    }
  };
}

sub clean_all_server ($$) {
  my ($cfg, $delete_data) = @_;

  # killall server
  for_hash $cfg, sub {
    my ($value) = @_;
    if ($value =~ /^\s*(\d+\.\d+\.\d+\.\d+)\s*$/) {
      my $cmd = "killall -u admin -r \"chunkserver|mergeserver|rootserver|updateserver\";";
      do_ssh($value, $cmd);
    }
  };

  if (defined $delete_data and $delete_data) {
    map { delete_cluster_data($cfg->{$_}) } @clusters;
  }
}

sub bootstrap ($$) {
  my ($ip, $port, $role) = @_;
  my $rs_admin = $global_cfg->{setting}{rs_admin};
  do_ssh($ip, "$rs_admin -r $ip -p $port -t 60000000 boot_strap");
}

sub set_obi_role ($$$) {
  my ($ip, $port, $role) = @_;
  my $rs_admin = $global_cfg->{setting}{rs_admin};
  do_ssh($ip, "$rs_admin -r $ip -p $port set_obi_role -o OBI_$role");
}

sub do_ssh {
  my ($ip, $cmd) = @_;
  $cmd = "cd /home/admin/oceanbase && $cmd";
  my $ssh_cmd = "ssh admin\@$ip '$cmd'";
  print "$ssh_cmd\n";
  system("$ssh_cmd");
}

sub check_all_ssh {
  my $cfg = shift;
  for_hash $cfg, sub {
    my ($value) = @_;
    if ($value =~ /^\s*(\d+\.\d+\.\d+\.\d+)\s*$/) {
      check_ssh($1);
    }
  };
}

#########################################################################
## read cfg file
sub read_cfg {
  # my $class = ref $_[0] ? ref shift : shift;

  # Check the file
  my $file = shift or die 'You did not specify a file name';
  die "File '$file' does not exist"               unless -e $file;
  die "'$file' is a directory, not a file"        unless -f _;
  die "Insufficient permissions to read '$file'"  unless -r _;

  # Slurp in the file
  local $/ = undef;
  open( CFG, $file ) or die die "Failed to open file '$file': $!";
  my $contents = <CFG>;
  close( CFG );

  read_string( $contents );
}

sub read_string {
  # Parse the string
  my $server      = 'public';
  my $counter = 0;
  my $group = undef;
  my $self = undef;
  foreach ( split /(?:\015{1,2}\012|\015|\012)/, shift ) {
    $counter++;

    if (/^\s*(\d+\.\d+\.\d+\.\d+)\s*$/) {
      my $index = 1 + grep { /^ip_/ } keys %{$self->{$group}{$server}};
      $_ = "ip_$index=$_";
    }

    # read group
    if (/^#\@begin_(.+)$/) {
      if (not $group) {
        $self->{$group = $1} ||= {};
        $server = 'public';
      } else {
        # multi begin group line
        return die( "Syntax error at line $counter: '$_'");
      }
      next;
    }

    if (/^#\@end_(.+)$/) {
      if ($group and $1 eq $group) {
        $server = 'public';
        $group = undef;
      } else {
        # multi end group line
        return die( "Syntax error at line $counter: '$_'");
      }
      next;
    }

    # Skip comments and empty lines
    next if /^\s*(?:\#|\;|$)/;

    # Remove inline comments
    s/\s\;\s.+$//g;

    # Handle section headers
    if ( /^\s*\[\s*(.+?)\s*\]\s*$/ ) {
      if ($group) {
        $self->{$group}{$server = $1} ||= {};
      } else {
        $self->{$server = $1} ||= {};
      }
      next;
    }

    # Handle properties
    if ( /^\s*([^=]+?)\s*=\s*(.*?)\s*$/ ) {
      if ($group) {
        $self->{$group}{$server}{$1} = $2;
      } else {
        $self->{$server }{$1} = $2;
      }
      next;
    }

    die "Syntax error at line $counter: '$_'";
  }
  $self;
}

sub get_master_rs {
  my $cfg = shift;
  my $master_cluster = $cfg->{global}{public}{master_cluster};
  ($cfg->{$master_cluster}{rootserver}{vip},
   $cfg->{$master_cluster}{rootserver}{port} || $cfg->{global}{rootserver}{port});
}

#############################################################################
## start server and cluster
sub start_rootserver($$) {
  ($_, my $rootserver) = @_;
  my $global_rs = $global_cfg->{rootserver};
  my $vip = $rootserver->{vip};
  my $port = $rootserver->{port} || $global_rs->{port};
  my $appname = $global_cfg->{public}{appname};
  my $cluster_id = $1 if (/cluster_(\d+)/);
  my $init_cfg = $init_cfg->{rootserver};
  my $devname = $rootserver->{devname} || $global_rs->{devname} || $global_cfg->{public}{devname} || "bond0";
  my $init_cfg_str = join ',',map { "$_=$init_cfg->{$_}" } keys %$init_cfg;
  my $commitlog_dir = $global_rs->{commitlog_dir};
  my $cmd = '';
  if ($action eq "init") {
    $cmd = "mkdir -p data/rs $commitlog_dir && ln -s $commitlog_dir data/rs_commitlog"
      . " && echo -e \"[app_name]\nname=$appname\nmax_table_id=1500\" >etc/schema.ini && ";
  }
  $cmd .= "./bin/rootserver -r $vip:$port -R $master_rs_vip:$master_rs_port -i $devname -C $cluster_id";
  $cmd .= " -o $init_cfg_str" if $init_cfg_str;
  map {
    do_ssh($rootserver->{$_}, $cmd);
  } grep { /ip_\d+/ } keys %$rootserver;
  ($vip, $port);
}

sub start_chunkserver($$$) {
  my ($chunkserver,$rs_vip,$rs_port) = @_;
  my $global_cs = $global_cfg->{chunkserver};
  my $cs_port = $chunkserver->{port} || $global_cs->{port};
  my $appname = $global_cfg->{public}{appname};
  my $init_cfg = $init_cfg->{chunkserver};
  my $init_cfg_str = join ',',map { "$_=$init_cfg->{$_}" } keys %$init_cfg;
  my $devname = $chunkserver->{devname} || $global_cs->{devname} || $global_cfg->{public}{devname} || "bond0";
  my $max_disk_num = $chunkserver->{max_disk_num} || $global_cs->{max_disk_num};
  my $cmd =  "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:/home/admin/oceanbase/lib";
  if ($action eq "init") {
    $cmd .= " && for ((i=1;i<=$max_disk_num;i++)) do mkdir -p /data/\$i/$appname/sstable; done"
      . " && mkdir -p data"
        . " && for ((i=1;i<=$max_disk_num;i++)) do ln -s /data/\$i data/\$i; done";
  }
  $cmd .= "&& ./bin/chunkserver -r $rs_vip:$rs_port -p $cs_port -n $appname -i $devname";
  $cmd .= " -o $init_cfg_str" if $init_cfg_str;
  map {
    do_ssh($chunkserver->{$_}, $cmd);
  } grep { /ip_\d+/ } keys %$chunkserver;
}

sub start_mergeserver($$$) {
  my ($mergeserver, $rs_vip, $rs_port) = @_;
  my $global_ms = $global_cfg->{mergeserver};
  my $ms_port = $mergeserver->{port} || $global_ms->{port};
  my $ms_sql_port = $mergeserver->{sql_port} || $global_ms->{sql_port};
  my $init_cfg = $init_cfg->{mergeserver};
  my $init_cfg_str = join ',',map { "$_=$init_cfg->{$_}" } keys %$init_cfg;
  my $devname = $mergeserver->{devname} || $global_ms->{devname} || $global_cfg->{public}{devname} || "bond0";
  my $cmd = "./bin/mergeserver -r $rs_vip:$rs_port -p $ms_port -z $ms_sql_port -i $devname";
  $cmd .= " -o $init_cfg_str" if $init_cfg_str;
  map {
    do_ssh($mergeserver->{$_}, $cmd);
  } grep { /ip_\d+/ } keys %$mergeserver;
}

sub start_updateserver($$$) {
  my ($updateserver, $rs_vip, $rs_port) = @_;
  my $global_ups = $global_cfg->{updateserver};
  my $ups_port = $updateserver->{port} || $global_ups->{port};
  my $ups_merge_port = $updateserver->{internal_port} || $global_ups->{inner_port};
  my $init_cfg = $init_cfg->{updateserver};
  my $init_cfg_str = join ',',map { "$_=$init_cfg->{$_}" } keys %$init_cfg;
  my $max_disk_num = $updateserver->{max_disk_num} || $global_ups->{max_disk_num};
  my $devname = $updateserver->{devname} || $global_ups->{devname} || $global_cfg->{public}{devname} || "bond0";
  my $commitlog_dir = $global_ups->{commitlog_dir};
  my $cmd = "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:/home/admin/oceanbase/lib";
  if ($action eq "init") {
    $cmd .= " && mkdir -p data/ups_data/raid{" . join(',', (0 .. ($max_disk_num - 1)/ 2)) . "}"
      . " && mkdir -p $commitlog_dir && ln -s $commitlog_dir data/ups_commitlog";
    map {
      $cmd .= " && ln -s /data/$_ data/ups_data/raid" . int(($_ - 1) / 2) . "/store" . ($_ - 1) % 2;
    } (1 .. $max_disk_num);
  }
  $cmd .= " && ./bin/updateserver -r $rs_vip:$rs_port -p $ups_port -m $ups_merge_port -i $devname"
    . ($init_cfg_str and " -o $init_cfg_str" or "");
  map {
    do_ssh($updateserver->{$_}, $cmd);
  } grep { /ip_\d+/ } keys %$updateserver;
}

sub start_fake_ms($$$) {
   my ($rootserver, $rs_vip, $rs_port) = @_;
   my $global_ms = $global_cfg->{mergeserver};
   my $lms_port = $global_ms->{lms_port} || '2828';
   my $ms_port = $global_ms->{port} || '2800';
   my $init_cfg = $init_cfg->{mergeserver};
   my $init_cfg_str = join ',',map { "$_=$init_cfg->{$_}" } keys %$init_cfg;
   my $devname = $global_ms->{devname} || $global_cfg->{public}{devname} || "bond0";
   my $cmd = "./bin/mergeserver -r $rs_vip:$rs_port -p $ms_port -z $lms_port -i $devname";
   $cmd .= " -o $init_cfg_str" if $init_cfg_str;
   map {
     do_ssh($rootserver->{$_}, $cmd);
   } grep { /ip_\d+/ } keys %$rootserver;
}

sub start_cluster {
  my ($cluster_name, $cluster) = @_;

  my ($rs_vip, $rs_port) = start_rootserver($cluster_name,
                                            $cluster->{rootserver});
  start_updateserver($cluster->{updateserver}, $rs_vip, $rs_port);
  start_chunkserver($cluster->{chunkserver}, $rs_vip, $rs_port);
  start_mergeserver($cluster->{mergeserver}, $rs_vip, $rs_port);
  start_fake_ms($cluster->{rootserver}, $rs_vip, $rs_port);

  if ($rs_vip eq $master_rs_vip && $rs_port eq $master_rs_port) {
    set_obi_role($rs_vip, $rs_port, "MASTER");
    bootstrap($rs_vip, $rs_port);
  } else {
    set_obi_role($rs_vip, $rs_port, "SLAVE");
  }
}

sub get_one_ms {
  my $cfg = shift;
  my $master_cluster = $cfg->{global}{public}{master_cluster};
  ($cfg->{$master_cluster}{rootserver}{vip},
   $cfg->{global}{mergeserver}{lms_port});
}

sub verify_bootstrap
{
    my $cfg = shift;
    my $lms_ip;
    my $lms_port;
    ($lms_ip, $lms_port) = get_one_ms($cfg);

    my @clusters = sort grep { /cluster_\d+/ } keys %$cfg;
    my $cluster_num = $#clusters + 1;
    print "$lms_ip, $lms_port, $cluster_num\n";
    my $dbh = DBI->connect("DBI:mysql:database=test;host=${lms_ip};port=${lms_port}",
			   "admin", "admin",
			   {'RaiseError' => 1});

    # 1. verify __all_cluster
    my $sth = $dbh->prepare("SELECT cluster_id,cluster_vip,cluster_port,cluster_role FROM __all_cluster");
    $sth->execute();
    my $row_count = 0;
    while (my $ref = $sth->fetchrow_hashref()) {
	print Dumper($ref);
	my $cluster_id = $ref->{'cluster_id'};
	my $cluster_vip = $ref->{'cluster_vip'};
	my $cluster_role = $ref->{'cluster_role'};
	my $cluster_port = $ref->{'cluster_port'};
	my $cluster_name = "cluster_${cluster_id}";
	($cfg->{$cluster_name}{rootserver}{vip} eq $cluster_vip) || die "ERROR: cluster $cluster_id's vip";
	($cfg->{global}{mergeserver}{lms_port} eq $cluster_port) || die "ERROR: cluster $cluster_id's vip";
	my $master_cluster_id = $1 if ($cfg->{global}{public}{master_cluster} =~ /cluster_(\d+)/);
	if ($cluster_id = $master_cluster_id)
	{
	    ($cluster_role == 1) || die "ERROR: invalid cluster role for master cluster\n";
	}
	else
	{
	    ($cluster_role == 2) || die "ERROR: invalid cluster role for slave cluster\n";
	}
	++ $row_count;
    }
    ($row_count == $cluster_num) || die "ERROR: some cluster is lost in __all_cluster";
    $sth->finish();

    # 2. verify __all_server;
    my $sth2 = $dbh->prepare("SELECT cluster_id,svr_type,svr_ip,svr_port,inner_port from __all_server");
    $sth2->execute();
    $row_count = 0;
    while (my $ref = $sth2->fetchrow_hashref()) {
	print Dumper($ref);
	my $cluster_id = $ref->{cluster_id};
	my $svr_type = $ref->{svr_type};
	my $svr_ip = $ref->{svr_ip};
	my $svr_port = $ref->{svr_port};
	my $inner_port = $ref->{inner_port};
	my $cluster_name = "cluster_${cluster_id}";
	my $servers = $cfg->{$cluster_name}{$svr_type};
	my $found = 0;
	for my $s (keys %$servers){
	    if ($s = $svr_ip){
		$found = 1;
		last;
	    }
	}
	$found || die "ERROR: unknown $svr_type $svr_ip in __all_server";
	if ($svr_type eq "rootserver" || $svr_type eq "chunkserver")
	{
	    ($cfg->{global}{$svr_type}{port} == $svr_port) || die "ERROR: $svr_type port $svr_port";
	}
	elsif ($svr_type eq "mergeserver")
	{
	    ($cfg->{global}{$svr_type}{port} == $inner_port) || die "ERROR: wrong mergeserver port $inner_port";
	    ($cfg->{global}{$svr_type}{sql_port} == $svr_port) || die "ERROR: wrong mergeserver port $inner_port";
	}
	else
	{
	    ($cfg->{global}{$svr_type}{port} == $svr_port) || die "ERROR: wrong updateserver port $svr_port";
	    ($cfg->{global}{$svr_type}{inner_port} == $inner_port) || die "ERROR: wrong updateserver port $inner_port";
	}
	++ $row_count;
    }
    my $servers_num = 0;
    my @server_type = ("rootserver", "chunkserver", "updateserver", "mergeserver");
    for my $c (@clusters){
	for my $t (@server_type){
	    my @ips = grep { /ip_\d+/ } keys %{$cfg->{$c}{$t}};
	    $servers_num += $#ips + 1;
	}
    }
    ($servers_num == $row_count) || die "ERROR: some server not in __all_server, we have $servers_num servers";
    $sth2->finish();
    # done
    $dbh->disconnect();
    print "Verify OKay.\n";
}

sub quicktest
{
    my $cfg = shift;
    my $lms_ip;
    my $lms_port;
    ($lms_ip, $lms_port) = get_one_ms($cfg);

    for my $test ("create", "show", "count_distinct", "join_basic", "group_by_1", "sq_from", "ps_complex"){
        print "[TEST] $test\n";
        my $output = `bin/mysqltest --logdir=tests --port=$lms_port --tmpdir=tests --database=test --timer-file=tests --user=admin --host=$lms_ip --result-file=tests/${test}.result --test-file=tests/${test}.test --tail-lines=10 --password=admin --silent`;
        print "$output\n";
    }
}

sub run_mysql
{
    my $cfg = shift;
    my $lms_ip;
    my $lms_port;
    ($lms_ip, $lms_port) = get_one_ms($cfg);
	`mysql -h $lms_ip -P $lms_port -u admin -p admin`;
}

sub main {
  pod2usage(1) if ((@ARGV < 2) && (-t STDIN));
  my $cfg_file = pop @ARGV;
  $action = shift @ARGV;

  my $help = '';
  my $force = '';
  my $cluster = '';
  my $result = GetOptions("force" => \$force,
                          "c|cluster=s" => \$cluster
                         ) or pod2usage(1);
  pod2usage(1) if (not $action =~ m/^dump$|^init$|^clean$|^stop$|^start$|^check$|^mysql$/);

  my $cfg = read_cfg($cfg_file);
  $global_cfg = $cfg->{global};
  $init_cfg = $cfg->{init_config};
  ($master_rs_vip, $master_rs_port) = get_master_rs($cfg);
  if ($cluster) {
    if ($cluster =~ /cluster_\d+/ and $cfg->{$cluster}) {
      @clusters = ($cluster);
    } else {
      print "Cluster not validated! [$cluster]\n";
      exit(-1);
    }
  } else {
    @clusters = sort grep { /cluster_\d+/ } keys %$cfg;
  }

  if ($action eq "dump") {
    print Dumper($cfg);
    exit(0);
  }
  if ($action eq "clean") {
    if (not $force) {
      $|=1;
      print "Will *DELETE ALL DATA* from servers, sure? [y/N] ";
      read STDIN, my $char, 1;
      exit (0) if $char ne 'y' and $char ne 'Y';
    }

    check_all_ssh $cfg;
    clean_all_server $cfg, "1";
    exit(0);
  }
  if ($action =~ "init|start") {
    if (not $force) {
      $|=1;
      print "Will *DELETE ALL DATA* from servers, sure? [y/N] ";
      read STDIN, my $char, 1;
      exit (0) if $char ne 'y' and $char ne 'Y';
    }

    check_all_ssh $cfg;
    map { start_cluster($_, $cfg->{$_}) } @clusters;
    exit(0);
  }
  if ($action eq "stop") {
    if (not $force) {
      $|=1;
      print "Stop all server!! Sure? [y/N] ";
      read STDIN, my $char, 1;
      exit (0) if $char ne 'y' and $char ne 'Y';
    }
    clean_all_server $cfg, '';
  }
  if ($action eq "check") {
      verify_bootstrap $cfg;
	  quicktest $cfg;
  }
  if ($action eq "mysql") {
      run_mysql $cfg;
  }
}

main;

__END__

=head1 NAME

    oceanbase.pl - a script to deploy oceanbase clusters.

=head1 SYNOPSIS

oceanbase.pl start|stop|init|clean|dump [Options] cfg_file

=head1 OPTIONS

=item B<--cluster,-c> CLUSTER_ID

All actions is for that cluster.

=item B<--force>

Force run command without asking anything.

=head2 clean

=over 2

Clean all data and B<STOP> all server.

=head2 init

Create all directory and links and start oceanbase and init.

=head2 start

Only start servers

=head2 stop

Stop all servers

=head2 dump

Dump configuration read from cfg_file

=back

=head1 AUTHOR

Yudi Shi - L<cedsyd@gmail.com>

=head1 DESCRIPTION

=cut
