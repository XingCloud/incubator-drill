#!/usr/bin/perl
use Cwd 'abs_path';
use File::Basename;

$cmd = $ARGV[0];
$baseDir =  dirname(dirname(abs_path(__FILE__))) ;
$confDir = $baseDir . "/conf" ;
$libDir = $baseDir . "/lib";
$logDir = "/data/log/drill" ;
$DRIBIT_PID = $baseDir . "/drill/drillbit.pid" ;
$JVMFLAGS="-XX:OnOutOfMemoryError='perl $baseDir/bin/service.pl stop' -XX:+UseG1GC -XX:MaxDirectMemorySize=8g -Xmx6g -Xms2g";
$JAVA_HOME = "/usr/java/jdk1.7.0_40/" ;
if($cmd eq 'start'){
	&start;
}elsif($cmd eq 'stop'){
	&stop;
}elsif($cmd eq 'restart'){
	&restart;
}elsif($cmd eq 'status'){
	&status;
}else{
	&help;
}

sub start{
	print "Starting drillbit ...\n";
	if(-e $DRIBIT_PID){
		print "Drillbit already running as process " . `cat $DRIBIT_PID` ;
		return;
	}

	$CLASS = "org.apache.drill.exec.server.Drillbit" ;
	# -Dio.netty.noResourceLeakDetection=true
	$SP = "-Djava.library.path=/usr/lib/hadoop/lib/native:/home/hadoop/hbase-single/bin/../lib/native/Linux-amd64-64" ;
	$LOG = "$logDir/drill-bit.log";
	open (FILE,"<","$confDir/classpath") or die $!;
	$CP = <FILE> ;
	chomp($CP);
	chdir($libDir);
	`nohup $JAVA_HOME/bin/java $JVMFLAGS $SP -cp $CP $CLASS -c $confDir/drill-module.conf> /dev/null 2>$logDir/error.log  </dev/null &` ;
	$pid = `$JAVA_HOME/bin/jps|grep 'Drillbit'|awk -F ' ' '{print \$1}'`;
	chomp($pid);
	`/bin/echo  $pid > $DRIBIT_PID`;
}

sub stop{
	print "Stopping drillbit ...\n";
	$pid = `$JAVA_HOME/bin/jps|grep 'Drillbit'|head -1|awk -F ' ' '{print \$1}'`;
	chomp $pid;
	if(defined $pid and $pid ne ''){
		print "kill $pid\n";
		`kill -9 $pid`;
	}else{
		print "No drillbit running.\n" ;
	}
	`rm -f $DRIBIT_PID` if -e $DRIBIT_PID;
}

sub restart{
	print "Restart drillbit ... \n";
	&stop;
	sleep 1;
	&start;
}

sub status{
	$pid = `$JAVA_HOME/bin/jps|grep 'Drillbit'|head -1|awk -F ' ' '{print \$1}'`;
	chmod $pid ;
	if(-e $DRIBIT_PID or (defined $pid and $pid ne '')){
		print "Drillbit is running.\n";
	}else{
		print "Drillbit is not running.\n";
	}
}

sub help{
	print "Usage: $0 {start|stop|restart|status}\n" ;
}
