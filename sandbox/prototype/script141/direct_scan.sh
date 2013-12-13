for i in `cat /home/hadoop/xa/scripts/ip_16.txt`;do
echo $i
ssh $i perl ~/Drill/bin/ds.pl deu_age 20130901visit.22find.hp. 20130901visit. false false
done
