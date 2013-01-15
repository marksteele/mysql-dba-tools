#!/usr/bin/perl
use DBD::mysql;
use Data::Dumper;
use IO::Compress::Gzip qw(gzip $GzipError :constants);
use POSIX qw(nice uname);
use Getopt::Long;
use File::Basename;
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
my $hostname = (uname())[1];
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
  "numslaves=i" => \$numslaves,
  "priority=i" => \$priority,
  "port=i" => \$port,
  "help" => \$help,
  "purge" => \$purge,
  "keep=i" => \$keep,
);

if (!$numslaves || !$keep || $keep < 1) {
  print "Error: You must specify the number of slaves this master has, and the number of days worth of logs you wish to keep\n\n";
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
unlink("/var/run/$me.pid");


