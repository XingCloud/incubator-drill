#!/usr/bin/perl

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

foreach $node (@nodes){
	print $node . ":" . `ssh $node " grep 'Exception' /data/log/drill/stdout.log.2013-11-27.log |tail -n 5"`. "\n" ;
}

