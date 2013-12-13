#!/usr/bin/perl
$branch = $ARGV[0] ;
$branch = 'i20' if not defined $branch ;
`JAVA_HOME=/usr/java/jdk1.7.0_25/`;
chdir '/home/hadoop/Drill/incubator-drill/sandbox/prototype/exec/java-exec';

print `git fetch origin`;
print `git checkout $branch`;
print `git merge origin/$branch`;
print `mvn package -DskipTests`;
print `cp target/java-exec-1.0-SNAPSHOT-rebuffed.jar ~/Drill/java-exec-1.0-SNAPSHOT.jar` ;
print `perl /home/hadoop/Drill/script/scp.pl /home/hadoop/Drill/java-exec-1.0-SNAPSHOT.jar /home/hadoop/Drill/`;
print `perl /home/hadoop/Drill/script/service.pl restart`;
