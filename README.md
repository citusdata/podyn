# DynamoDB to Citus migration

The `dynamodb-to-postgres` tool migrates a DynamoDB table to PostgreSQL or Citus by creating and adjusting the table schema, copying the data, and then continuously replaying changes.

## Building from source

To build a shaded JAR, run:

```
mvn package
```

The JAR file will be at `target/dynamodb-to-postgres-1.0.jar`.

## Migrate data from DynamoDB

Once you've built the JAR, you can run it as follows.

```
java -jar target/dynamodb-to-postgres-1.0.jar --help
usage: dynamodb-to-postgres
 -c,--no-changes     Skip streaming changes
 -d,--no-data        Skip initial data load
 -h,--help           Show help
 -m,--scan-rate      Maximum reads/sec during scan
 -r,--region <arg>   AWS region
 -s,--no-schema      Skip initial schema creation
 -t,--table <arg>    DynamoDB table name
```

After [setting up your AWS credentials](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default), you can migrate a table by running:

```
java -jar target/dynamodb-to-postgres-1.0.jar --table mytable
```
