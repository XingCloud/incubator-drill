#!/usr/bin/perl

$from = $ARGV[0];
$to = $ARGV[1];
@nodes = (
	'node0',
	'node1',
	'node2',
	'node3',
	'node4',
	'node5',
	'node6',
	'node7',
	'node8',
	'node9',
	'node10',
	'node11',
	'node12',
	'node13',
	'node14',
	'node15');

print "from: $from , to: $to\n" ;
print "$_:\n" . `scp $from hadoop\@$_:$to ` foreach (@nodes) ;
