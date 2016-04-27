#S3 distributed reader

Provides distributed S3 key reading facility with high-availability and exactly-once guarantee.

##Locking
When multiple instances need to read keys from S3 bucket (in a distributed
fashion) the instances need to agree and coordinate on processing the files so that
all files are processed eventually and each file is processed only once.

S3 does not provide key locking nor it provides strong consistency over key values.
Therefore a common technique to create 'lock files' does not work and mutual exclusion
of multiple instances trying to access single file must be carried out externally. Such
an external store can be a relational database or any datastore with strong consistency.

This project implements S3 readers using AWS SDK and mutual exclusion by Zookeeper.

##Running

Make sure that Zookeeper Server is running at hostname and port as given in the configuraiton.

##Testing scenarios (to be completed)

1. read S3Listing - start from current Index - 4 partitions

2. acquire lock file at current index

3. mark the file processed

4. release lock

5. process to next index until it reaches startIndex - 1 % totalSize

A. Test without partitioning

0. Genereate files at S3

1. Basic scenario
- run 4 OK instances 4-0
	- make sure that all files get processed
	- make sure that each file was processed only once

2. Failure scenario 3-1
	- run 3 OK instances, 1 failing instance
	- make sure that all files get processed
	- make sure that each file was processed only once

3. Failure scenario 1-3
	- run 1 ok instance, 3 failing instances
	- make sure that all files get processed
	- make sure that each file was processed only once

4. Failure scenario 0-4
	- make sure that none of the files get processed

Issues:
	- testing - two processes do not process the same file
	- failed instance does not keep the file locked - another instance will use process file belonging to the other instance
