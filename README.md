# DynamoDB to PostgreSQL / Citus replication

The `podyn` tool replicates DynamoDB tables to PostgreSQL tables, which can optionally be distributed using Citus. It can also keep the tables in sync by continuously streaming changes.

## Building from source

To build a shaded JAR, run:

```
git clone https://github.com/citusdata/podyn.git
cd podyn
mvn package
```

The JAR file will be at `target/podyn-1.0.jar`.

## Running the JAR file

Once you've built the JAR, you can run it as follows.

```
./podyn --help
usage: podyn
 -c,--changes                    Continuously replicate changes
 -d,--data                       Replicate the current data
 -h,--help                       Show help
 -lc,--lower-case-column-names   Use lower case column names
 -m,--conversion-mode <arg>      Conversion mode, either columns or jsonb (default: columns)
 -n,--num-connections <arg>      Database connection pool size (default 16)
 -r,--scan-rate <arg>            Maximum reads/sec during scan (default 25)
 -s,--schema                     Replicate the table schema
 -t,--table <arg>                DynamoDB table name(s) to replicate
 -u,--postgres-jdbc-url <arg>    PostgreSQL JDBC URL of the destination
 -x,--citus                      Create distributed tables using Citus
```

When `--postgres-jdbc-url` is omitted, the SQL statements that would otherwise be sent to the database are sent to stdout. When `--table` is omitted, all DynamoDB tables in the region are replicated.

## Replicate schema and data from DynamoDB

After [setting up your AWS credentials](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default), you can replicate the schema and do an initial data load by running:

```
export AWS_REGION=us-east-1

./podyin --postgres-jdbc-url "jdbc:postgresql://host:5432/citus?sslmode=require&user=citus&password=pw" --schema --data --citus

Constructing table schema for table events
Moving data for table events
Adding new column to table events: name text
Adding new column to table events: payload jsonb
```

When `--schema` is specified, tables will be created in PostgreSQL as described in the *Schema conversion rules* section. When the `--data` argument is specified, all the data in the DynamoDB table is scanned in batches. Before a batch is sent to postgres, any fields that did not appear in the existing schema are added as new columns. After the new columns are added, `COPY` is used to load the batch into postgres.

## Stream changes from DynamoDB

After schema creation and the initial data load, you can continuously stream changes using:

```
./podyn --postgres-jdbc-url "jdbc:postgresql://host:5432/citus?sslmode=require&user=citus&password=pw" --changes --citus

Replicating changes for table events
...
```

The changes are processed in batches and new fields are added to the table as columns. The changes are translated into delete  or upsert statements that are sent to postgres over multiple  connections (specified using `-n`) to achieve high throughput.

When running the command immediately after a data load, some changes that were made prior to the data load may be re-applied, causing the replicated database to temporarily regress. However, since the changes are applied in the same order they will eventually arrive at the current value. After loading a batch of changes into the database, a checkpoint is made. If the tool is restarted, it will continue from its last checkpoint. The checkpoints are stored in DynamoDB tables prefixed with `d2p_migration_`. 

For high availability, it is possible to replicate changes from multiple nodes concurrently, in which case the work will be evenly divided. If a node fails, the other node(s) will automatically take over its work.

## Schema conversion rules

Top-level keys in DynamoDB items are translated into columns. The initial schema is derived from the partition key, sort key and secondary indexes. When using Citus, the primary partition key becomes the distribution column.

DynamoDB types are mapped to PostgreSQL types according to the following table:

| DynamoDB type | PostgreSQL type |
| ------------- | --------------- |
| String        | text            |
| Binary        | bytea           |
| Numeric       | numeric         |
| StringSet     | jsonb           |
| NumberSet     | jsonb           |
| BinarySet     | jsonb           |
| Map           | jsonb           |
| List          | jsonb           |
| Boolean       | boolean         |
| Null          | text            |

For example, a DynamoDB table named `clicks` has primary partition key: `sitename` (String), primary sort key: `time` (String), and a secondary index named `pageid-index` on: `pageid` (Numeric). The following statements will be sent to PostgreSQL:

```
CREATE TABLE clicks (
   sitename text NOT NULL,
   "time" text NOT NULL,
   pageid numeric NOT NULL,
   PRIMARY KEY (site_name, "time")
);
CREATE INDEX "pageid-index" ON clicks (pageid);
SELECT create_distributed_table('clicks', 'siteid');
```

A new column is added to the PostgreSQL table whenever a new key appears in an item during the initial data load or while replicating changes. For example, if a `payload` (String) key is encountered, the following command would be sent to the database:

```
ALTER TABLE clicks ADD COLUMN payload text;
```

In DynamoDB, the same key can appear with different types as long as it's it's not part of the primary key or a secondary index. If a key appears that maps to a different PostgreSQL type then the type name is added as a suffix. For example, if an item appears which contains `"clicks" -> true`, then a new column would be added:

```
ALTER TABLE clicks ADD COLUMN payload_boolean boolean;
```

There are currently no guarantees in terms of which type is used for the 'main' column. Using the same key for different (postgres) types is best avoided.
