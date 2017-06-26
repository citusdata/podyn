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
 -h,--help                      Show help
 -nc,--no-changes               Skip streaming changes
 -nd,--no-data                  Skip initial data load
 -ns,--no-schema                Skip initial schema creation
 -r,--region <arg>              AWS region
 -sr,--scan-rate                Maximum reads/sec during scan (default 25)
 -t,--table <arg>               DynamoDB table name of the source
 -u,--postgres-jdbc-url <arg>   PostgreSQL JDBC URL of the destination
```

After [setting up your AWS credentials](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default), you can migrate a table by running:

```
java -jar target/dynamodb-to-postgres-1.0.jar --table mytable
```
