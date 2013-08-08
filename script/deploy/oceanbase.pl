#!/usr/bin/perl -w

use Data::Dumper;
use Getopt::Long;
use Pod::Usage;
use vars qw($master_rs_vip $master_rs_port $global_cfg $init_cfg $action);
use strict;

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

sub clean_all_server ($$) {
  my ($cfg, $delete_data) = @_;
  for_hash $cfg, sub {
    my ($value) = @_;
    if ($value =~ /^\s*(\d+\.\d+\.\d+\.\d+)\s*$/) {
      my $cmd = "killall -u admin -r \"chunkserver|mergeserver|rootserver|updateserver\";";
      $cmd .= " rm -rf /data/*/* /home/admin/oceanbase/{log,data,run}" if defined $delete_data and $delete_data;
      do_ssh($value, $cmd);
    }
  };
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
  my $cmd = '';
  if ($action eq "init") {
    $cmd = "mkdir -p data/rs data/rs_commitlog"
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
  my $cmd = "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:/home/admin/oceanbase/lib";
  if ($action eq "init") {
    $cmd .= " && mkdir -p data/ups_data/raid{" . join(',', (0 .. ($max_disk_num - 1)/ 2)) . "}"
      . " && mkdir -p data/ups_commitlog";
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

sub start_cluster {
  my ($cluster_name, $cluster) = @_;

  my ($rs_vip, $rs_port) = start_rootserver($cluster_name,
                                            $cluster->{rootserver});
  start_updateserver($cluster->{updateserver}, $rs_vip, $rs_port);
  start_chunkserver($cluster->{chunkserver}, $rs_vip, $rs_port);
  start_mergeserver($cluster->{mergeserver}, $rs_vip, $rs_port);

  if ($rs_vip eq $master_rs_vip && $rs_port eq $master_rs_port) {
    set_obi_role($rs_vip, $rs_port, "MASTER");
    bootstrap($rs_vip, $rs_port);
  } else {
    set_obi_role($rs_vip, $rs_port, "SLAVE");
  }
}

sub main {
  pod2usage(1) if ((@ARGV < 2) && (-t STDIN));
  my $cfg_file = pop @ARGV;
  $action = shift @ARGV;
  my @clusters;

  my $help = '';
  my $force = '';
  my $delete_data = '';
  my $cluster = '';
  my $result = GetOptions("force" => \$force,
                          "delete_data" => \$delete_data,
                          "c|cluster=s" => \$cluster
                         ) or pod2usage(1);
  pod2usage(1) if (not $action =~ m/^dump$|^init$|^clean$|^stop$|^start$/);

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

