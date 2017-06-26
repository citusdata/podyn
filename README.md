# DynamoDB to Citus migration

The `dynamodb-to-citus` tool migrates a DynamoDB table to Citus by creating and adjusting the table schema, copying the data, and then continuously replaying changes.

```
usage: dynamodb-to-citus --table <table name>
 --help                   Show help
 --table <arg>            DynamoDB table name
 --scan-rate              Maximum reads/sec during scan
 
 --no-schema              Skip initial schema creation
 --no-data                Skip initial data load
 --no-changes             Skip streaming changes
```
