#!/usr/bin/perl
use DBD::mysql;
use Data::Dumper;
use IO::Compress::Gzip qw(gzip $GzipError :constants);
use POSIX qw(nice);
use Getopt::Long;
use strict;

my $user = 'root';
my $pass = '';
my $host = 'localhost';
my $datadir = '/var/lib/mysql';
my $hostname = '';
my $numslaves = 0;
my $priority = 19;
my $port = 3306;
my $help;
my $purge;
my $keep;

my $result = GetOptions(
  "user=s" => \$user,
  "pass=s" => \$pass,
  "host=s" => \$host,
  "datadir=s" => \$datadir,
  "hostname=s" => \$hostname,
  "numslaves=i" => \$numslaves,
  "priority=i" => \$priority,
  "port=i" => \$port,
  "help" => \$help,
  "purge" => \$purge,
  "keep=i" => \$keep,
);

if (!$hostname || !$numslaves || !$keep) {
  print "Error: 
  You must specify the hostname (eg: datingdb19) that is the prefix to the mysql binlog files 
  (eg: /var/lib/mysql/datingdb19-bin.XXX), the number of slaves this master has, and the number 
  of days worth of logs you wish to keep
  ";
  usage();
}

if ($help) {
  usage();
}
sub usage {
  print <<"EOF";
Usage: $0 <options>

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

  --hostname=<the name of this host>

    The hostname string mysql thinks this hostname is at. Examine the binary log names to find this.

  --numslaves=<number of slaves this master has>

    The number of slaves this master has.

  --priority=<process niceness>

    The scheduling priority to use.  1-20 makes it schedule less often meaning it's nicer, -1 to -20 makes it schedule more often, meaning it will use resources more aggresively. Defaults to 19.

  --purge
    
    Purge the master logs when done. Be mindful of using this if you're not running this script often.

  --keep=<days>
 
  The number of days worth of compressed logs to keep around

EOF
  exit;
}

## Lower process priority to have a bit less impact on running system.
nice($priority);

my $dbh = DBI->connect("DBI:mysql:database=mysql;host=$host;port=$port", $user, $pass) || die("Couldn't connect: $!");

print "Grabbing master status\n";
my $status = $dbh->selectrow_hashref("SHOW MASTER STATUS");
$status->{'File'} =~ /\.(\d+)$/;

my $currentfileno = $1;

print "Current master binlog file is $1\n";

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
    print "Slave lag, retrying in a second\n";
    sleep 1;
    next;
  }
  print "Starting to compress files\n";
  opendir(D,$datadir) || die("Couldn't open data dir: $!");
  ## sort based on numerical comparison of binary log filename
  my @files = map { $_->[0] } sort { $a->[1] <=> $b->[1] } map { [$_, /\.(\d+)$/] } grep {/^$hostname-bin\.\d+$/}  readdir(D);
  closedir(D);
  foreach my $file (@files) {
    next if ($file !~ /^$hostname-bin\.(\d+)$/ || $1 >= $currentfileno || -e "$datadir/$file.gz");
    print "Compressing $file\n";
    gzip("$datadir/$file" => "$datadir/$file.gz",-Level=> Z_BEST_SPEED) || die("Error compression file $file: $GzipError\n");
    my  ($atime, $mtime) = (stat("$datadir/$file"))[8,9];
    utime($atime,$mtime,"$datadir/$file.gz");
    if ($purge) {
      print "Purging to $file\n";
      $dbh->do("PURGE BINARY LOGS TO ?",undef,$file);
    }
  }
  last;
}

if ($purge) {
  my $keeptime = time() - (86400*$keep);
  opendir(D,$datadir) || die ("couldn't open data dir: $!");
  my @files = map { $_->[0] } sort { $a->[1] <=> $b->[1] } map { [$_, /\.(\d+)$/] } grep {/^$hostname-bin\.\d+\.gz$/} readdir(D);
  closedir(D);
  foreach my $file (@files) {
    my $mtime = (stat("$datadir/$file"))[9];
    if ($mtime < $keeptime) {
      print "Removing archive $datadir/$file $mtime < $keeptime\n";
      unlink("$datadir/$file") || die ("Could not delete $datadir/$file");
    } else {
      print "Keeping $datadir/$file\n";
    }
  }
}
