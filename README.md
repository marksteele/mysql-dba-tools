mysql-dba-tools
===============

Tools to help manage mysql DBs


==============

rotatebinlogs.pl: Allows the compression and rotate of binary logs, checking to make sure expected number of slaves are synched before doing anything.

Sample usage:

root@dbserver1# ./rotatelogs.pl --purge --hostname=dbserver1 \
--numslaves=2 --host=localhost --user=root --pass=mypass \
--datadir=/var/lib/mysql --priority=10 --keep=4


Usage: ./rotatebinlogs.pl <options>

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
