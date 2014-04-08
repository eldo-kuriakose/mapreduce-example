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


**How to run**

Use <code> mvn eclipse:eclipse </code> to create an eclipse project ,then import the project to Eclipse workspace ,export an executable jar file from Eclipse .

**hadoop jar** <code>path-to-jar-file</code> <code> database-server</code> <code>database</code> <code>user</code> <code>password</code> <code>output-folder-path</code>
