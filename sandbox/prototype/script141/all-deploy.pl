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

`perl zk.pl stop`;
`perl service.pl stop`;
`ssh $_ rm -rf Drill` foreach(@nodes);
`ssh $_ mkdir  Drill` foreach(@nodes);
`scp drill.tar.gz hadoop\@$_:~/Drill/` foreach(@nodes);
`ssh $_ tar -zxvf ~/Drill/drill.tar.gz -C ~/Drill/` foreach(@nodes) ;
