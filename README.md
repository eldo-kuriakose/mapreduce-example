mapreduce-example
=================

Find out total purchase by each customer ,suppose you have a mysql table (name purchase) having following columns, 

Id            | customer      |  amount
------------- | ------------- | --------------
1             | bob           | 100
2             | ben           |  50
3             | peter         | 500
4             | peter         | 300
5             | bob           | 100
6             | ben           | 100
7             | peter         | 200


the input to the mapper is table rows ,find out sum of all purchase amount for a customer in reduce phase .
