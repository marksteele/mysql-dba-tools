mysql-dba-tools
===============

Tools to help manage mysql DBs


==============

rotatebinlogs.pl: Allows the compression and rotate of binary logs, checking to make sure expected number of slaves are synched before doing anything. This can be especially useful on high-churn sites that produce lots of binary logs in relatively short periods of time.

Sample usage:

  root@dbserver1# ./rotatelogs.pl --purge \
  --numslaves=2 --host=localhost --user=root --pass=mypass \
  --datadir=/var/lib/mysql --priority=10 --keep=4


Usage: ./rotatebinlogs.pl <options>

Options:
  --user=[username]
    The username to use when connecting to the DB server

  --pass=[password]
    The password to use when connecting to the DB server

  --host=[host to connect to]
    The hostname to connect to

  --port=[mysql port]
    The port to use to connect to the DB

  --datadir=[path to binlogs]
    The file path to the binlog files

  --numslaves=[number of slaves this master has]
    The number of slaves this master has.

  --priority=[process niceness]
    The scheduling priority to use.  1-20 makes it schedule less often meaning it's nicer, -1 to -20 makes it schedule more often, meaning it will use resources more aggresively. Defaults to 19.

  --purge  
    Purge the master logs and remove old archives.

  --keep=[days]
    The number of days worth of compressed logs to keep around
