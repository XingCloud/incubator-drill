#!/usr/bin/perl
use Cwd 'abs_path';
use File::Basename;

$baseDir =  dirname(dirname(abs_path(__FILE__))) ;
$confDir = $baseDir . "/conf" ;
$libDir = $baseDir . "/lib";
$JVMFLAGS="-Xmx8g -Xmn2g -Xms4g";
print "$libDir   $confDir"

$JAVA_HOME = "/usr/java/jdk1.7.0_25" ;
$CLASS = "org.apache.hadoop.hbase.regionserver.DirectScanner" ;
$SP = "-Djava.library.path=/usr/lib/hadoop/lib/native:/home/hadoop/hbase-single/bin/../lib/native/Linux-amd64-64" ;
open (FILE,"<","$confDir/ds") or die $!;
$CP = <FILE> ;
chomp($CP);
chdir($libDir);
print `$JAVA_HOME/bin/java $JVMFLAGS $SP -cp $CP $CLASS $ARGV[0] $ARGV[1] $ARGV[2]  $ARGV[3] $ARGV[4]` ;

