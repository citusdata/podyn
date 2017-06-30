# DynamoDB to PostgreSQL / Citus replication

The `dynamodb-to-postgres` tool replicates DynamoDB tables to PostgreSQLs (optionally distributed using Citus) by creating and adjusting the table schema, copying the data, and then continuously replaying changes.

## Building from source

To build a shaded JAR, run:

```
mvn package
```

The JAR file will be at `target/dynamodb-to-postgres-1.0.jar`.

## Running the JAR file

Once you've built the JAR, you can run it as follows.

```
java -jar target/dynamodb-to-postgres-1.0.jar --help
usage: dynamodb-to-postgres
 -c,--changes                   Skip streaming changes
 -d,--data                      Skip initial data load
 -h,--help                      Show help
 -n,--num-connections <arg>     Database connection pool size (default 16)
 -r,--scan-rate <arg>           Maximum reads/sec during scan (default 25)
 -s,--schema                    Skip schema creation
 -t,--table <arg>               DynamoDB table name(s) to replicate
 -u,--postgres-jdbc-url <arg>   PostgreSQL JDBC URL of the destination
 -x,--citus                     Create distributed tables using Citus
```

## Replicate schema and data from DynamoDB

After [setting up your AWS credentials](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default), you can replicate the schema and do an initial data load by running:

```
export AWS_REGION=us-east-1

java -jar target/dynamodb-to-postgres-1.0.jar --postgres-jdbc-url jdbc:postgresql://host:5432/postgres?sslmode=require&user=citus&password=pw --schema --data --citus

Constructing table schema for table events
Moving data for table events
Adding new column to table events: name text
Adding new column to table events: payload jsonb
```

An initial CREATE TABLE statement is constructed from the partition key and secondary indexes of the DynamoDB tables. After that, the data in the DynamoDB table is scanned in batches. Before a batch is sent to postgres, any fields that did not appear in the existing schema are added as new columns. After the new columns are added, COPY is used to load the batch into postgres.

## Replicate changes from DynamoDB

After the command completes, you can continuously stream changes using:

```
java -jar target/dynamodb-to-postgres-1.0.jar --postgres-jdbc-url jdbc:postgresql://host:5432/postgres?sslmode=require&user=citus&password=pw --changes --citus

Replicating changes for table events
...
```

The changes are processed in batches and new fields are added to the table as columns. The changes are translated into DELETE or INSERT INTO .. ON CONFLICT (UPSERT) statements that are sent to postgres using a connection pool in such a way that the ordering of changes to the same key is preserved.

After loading a batch of changes into the database, a checkpoint is made. If the tool is restarted, it will continue from its last checkpoint. The checkpoints are stored in DynamoDB tables prefixed with `d2p_migration_`.
