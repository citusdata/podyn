# DynamoDB to Citus replication

The `dynamodb-to-postgres` tool replicates a DynamoDB table to PostgreSQL or Citus by creating and adjusting the table schema, copying the data, and then continuously replaying changes.

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
 -c,--citus                     Create distributed tables using Citus
 -h,--help                      Show help
 -n,--num-connections <arg>     Database connection pool size (default 16)
 -na,--no-add-columns           Do not add columns to the initial schema
 -nc,--no-changes               Skip streaming changes
 -nd,--no-data                  Skip initial data load
 -ns,--no-schema                Skip schema creation
 -r,--scan-rate <arg>           Maximum reads/sec during scan (default 25)
 -t,--table <arg>               DynamoDB table name(s) to replicate
 -u,--postgres-jdbc-url <arg>   PostgreSQL JDBC URL of the destination
```

After [setting up your AWS credentials](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default), you can replicate the schema and do an initial data load by running:

```
java -jar target/dynamodb-to-postgres-1.0.jar --postgres-jdbc-url jdbc:postgresql://host:5432/postgres?sslmode=require&user=citus&password=pw --no-changes --citus
```

After the command completes, you can continuously stream changes using:

```
java -jar target/dynamodb-to-postgres-1.0.jar --postgres-jdbc-url jdbc:postgresql://host:5432/postgres?sslmode=require&user=citus&password=pw --no-schema --no-data --citus
```
