#!/usr/bin/perl


$from = '/home/hadoop/Drill/conf/drill-module.conf';
$to = '/home/hadoop/Drill/conf/drill-module.conf';

$temp = '/home/hadoop/Drill/conf/drill-module.conf.temp';

print "from: $from , to: $to\n" ;
foreach (0..15){
	$node = "node$_";
	`sed 's/NODE/$node/g' $from > $temp`;
	print "$node:\n" . `scp $temp hadoop\@$node:$to ` ;
}

`rm -f $temp`;
