#!/usr/bin/perl

use threads ;

$cmd = $ARGV[0] ;

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

$SERVICE = '/home/hadoop/Drill/bin/service.pl' ;

@thrds = () ;

if($cmd eq 'start'){
	&start;
}elsif($cmd eq 'stop'){
	&stop;
}elsif($cmd eq 'status'){
	&status;
}elsif($cmd eq 'restart'){
	&restart;
}else{
	&help;
}

$_->join() foreach(@thrds) ;

sub thr{
	$node = shift ;
	$operator = shift ;
	print $node . ':' . `ssh $node /usr/bin/perl $SERVICE $operator` ;
}

sub start{
	push @thrds,threads->create(\&thr,($_,'start')) foreach(@nodes) ;
}

sub stop{
	push @thrds,threads->create(\&thr,($_,'stop')) foreach(@nodes) ;
}

sub status{
	push @thrds,threads->create(\&thr,($_,'status')) foreach(@nodes) ;
}

sub restart{
	print $_ . ':' .  `ssh $_ /usr/bin/perl $SERVICE restart` foreach(@nodes) ;
}

sub help{
        print "Usage: $0 {start|stop|restart|status}\n" ;
}
