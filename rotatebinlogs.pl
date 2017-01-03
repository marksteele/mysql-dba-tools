#!/usr/bin/perl
use DBD::mysql;
use Data::Dumper;
use IO::Compress::Gzip qw(gzip $GzipError :constants);
use POSIX qw(nice uname);
use POSIX 'strftime';
use Getopt::Long;
use File::Basename;
use Filesys::Df;
use strict;


my $me = basename($0);

if (-e "/var/run/$me.pid") {
  open(F,"/var/run/$me.pid") or die ("can't open $me.pid: $!");
  my $pid = <F>;
  close(F);
  if (-e "/proc/$pid/cmdline") {
    open(F,"/proc/$pid/cmdline") or die ("can't open /proc/$pid/cmdline: $!");
    my $cmd = <F>;
    close(F);
    if ($cmd =~ /$me/) {
      warn("Already running");
      exit;
    }
  }
}

open(F,">/var/run/$me.pid") or die ("can't open $me.pid: $!");
print F $$;
close(F);

my $user = 'root';
my $pass = '';
my $host = 'localhost';
my $datadir = '/var/lib/mysql';
my $prefix = '';
my $numslaves = 0;
my $priority = 19;
my $port = 3306;
my $help;
my $purge;
my $purge_free_space_threshold = 200; #in MB
my $keep;
my $uncompressed_keep = 24;
my $uncompressed_free_space_threshold = 100; #in MB

my $result = GetOptions(
  "user=s" => \$user,
  "pass=s" => \$pass,
  "host=s" => \$host,
  "datadir=s" => \$datadir,
  "numslaves=i" => \$numslaves,
  "priority=i" => \$priority,
  "port=i" => \$port,
  "help" => \$help,
  "purge" => \$purge,
  "purge_free_space_threshold=f" => \$purge_free_space_threshold,
  "keep=i" => \$keep,
  "uncompressed_keep=f" => \$uncompressed_keep,
  "uncompressed_free_space_threshold=f" => \$uncompressed_free_space_threshold,
  "prefix=s" => \$prefix,
);

if (!$numslaves || !$keep || $keep < 1 || !$prefix) {
  print "Error: You must specify the number of slaves this master has, and the number of days worth of logs you wish to keep\nYou also need to specify the binary log file name prefix (eg: mysqld if your binary logs are /usr/lib/mysql/mysqld-bin.XXXXX)\n";
  usage();
}

if ($help) {
  usage();
}
sub usage {
  print <<"EOF";
Usage: $me <options>

Options:
  --user=<username> 

    The username to use when connecting to the DB server

  --pass=<password>

    The password to use when connecting to the DB server

  --host=<host to connect to>

    The hostname to connect to

  --port=<mysql port>

    The port to use to connect to the DB

  --datadir=<path to binlogs>

    The file path to the binlog files

  --numslaves=<number of slaves this master has>

    The number of slaves this master has.

  --priority=<process niceness>

    The scheduling priority to use.  1-20 makes it schedule less often meaning it's nicer, -1 to -20 makes it schedule more often, meaning it will use resources more aggresively. Defaults to 19.

  --purge
    
    Purge the master logs when done. Be mindful of using this if you're not running this script often.

  --purge_free_space_threshold=<total_space_in_MB>

    It purges the archived logs when there is less then specified disk space avaiable. It's enabled only when --purge option is specified. The default is 200MB.

  --keep=<days>
 
    The number of days worth of compressed logs to keep around. The archived logs can be purged when there is not enough space available. See --purge_free_space_threshold.

  --uncompressed_keep=<hours>

    The number of hours worth of UNcompressed logs to keep around. The logs are purged only when the available space on filesystem is less then
    --uncompressed_free_space_threshold=<total_space_in_MB>. The default value is 24 hour.

  --uncompressed_free_space_threshold=<total_space_in_MB>

    See --uncompressed_keep=<hours>. The default is 100MB.

  --prefix=<prefix>

    The binary log file name prefix (eg: mysqld if your logs are /usr/lib/mysql/mysqld-bin.XXXXXX)

EOF
  exit;
}

## Lower process priority to have a bit less impact on running system.
nice($priority);

my $dbh = DBI->connect("DBI:mysql:database=mysql;host=$host;port=$port;mysql_read_default_file=$ENV{HOME}/.my.cnf", $user, $pass) || die("Couldn't connect: $!");

print "Grabbing master status\n";
my $status = $dbh->selectrow_hashref("SHOW MASTER STATUS");
print "Current master binlog file is ", $status->{'File'}, "\n";

for (my $retries = 0; $retries < 10; $retries++) {
  my $skip = 0;
  print "Grabbing processlist\n";
  my $plist = $dbh->selectall_arrayref("SHOW PROCESSLIST", { Slice => {}});
  my %slaves = ();
  for (@{$plist}) {
    $slaves{$_->{'Host'}} = $_->{'State'} if ($_->{'Command'} eq 'Binlog Dump');
  }
  foreach my $slave (keys %slaves) {
    if ($slaves{$slave} != 'Master has sent all binlog to slave; waiting for binlog to be updated') {      
      $skip = 1;
    }
  }
  if ($skip || scalar keys %slaves != $numslaves) {
    print "Slave lag or wrong number of slaves, retrying in a second\n";
    sleep 1;
    next;
  }

  print "Starting to compress files\n";

  my $files = $dbh->selectall_arrayref("SHOW MASTER LOGS", { Slice => {}});
  foreach my $file (@{$files}) {
    last if ($file->{'Log_name'} eq $status->{'File'});
    my $f = $datadir . '/' . $file->{'Log_name'};
    next if (!-e $f || -e "$f.gz");
    print "Compressing $f\n";
    gzip($f => "$f.gz",-Level=> Z_BEST_SPEED) || die("Error compression file $f: $GzipError\n");
    my  ($atime, $mtime) = (stat($f))[8,9];
    utime($atime,$mtime,"$f.gz");
    if ($purge) {
      print "Purging to $f\n";
      $dbh->do("PURGE BINARY LOGS TO ?",undef,$file->{'Log_name'});
    }
  }
  last;
}

if ($purge) {
  my $keeptime = time() - (86400*$keep);
  opendir(D,$datadir) || die ("couldn't open data dir: $!");
  my @files = map { $_->[0] } sort { $a->[1] <=> $b->[1] } map { [$_, /\.(\d+)\.gz$/] } grep {/^$prefix\.\d+\.gz$/} readdir(D);
  closedir(D);
  foreach my $file (@files) {
    my $mtime = (stat("$datadir/$file"))[9];
    my $datadir_df = df($datadir, 1024*1024); # in MB
    if ($mtime < $keeptime) {
      print "Removing archive $datadir/$file $mtime < $keeptime\n";
      unlink("$datadir/$file") || die ("Could not delete $datadir/$file");
    } elsif (defined($datadir_df) && ($datadir_df->{bavail} <= $purge_free_space_threshold)) {
      print "Removing archive $datadir/$file because there is less then ${purge_free_space_threshold}MB available\n";
      unlink("$datadir/$file") || die ("Could not delete $datadir/$file");
    } else {
      print "Keeping $datadir/$file\n";
    }
  }
}

# remove uncompressed binlogs when too less space is available
my $datadir_df = df($datadir, 1024*1024); # in MB
if(defined($datadir_df)) {
  if($datadir_df->{bavail} <= $uncompressed_free_space_threshold) {
    my $now = time();
    my $purge_time = $now - ($uncompressed_keep*3600);
    my $purge_time_formatted = POSIX::strftime( '%Y-%m-%d %T', localtime($purge_time) );
    print "Purging uncompressed binlogs older than ${uncompressed_keep} hours (before ${purge_time_formatted}) because there is less then ${uncompressed_free_space_threshold}MB available\n";
    $dbh->do("PURGE BINARY LOGS BEFORE ?",undef,$purge_time_formatted);
  }
}


unlink("/var/run/$me.pid");


