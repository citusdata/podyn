# Podyn: DynamoDB to PostgreSQL replication

The `podyn` tool replicates DynamoDB tables to PostgreSQL tables, which can optionally be distributed using Citus. It can also keep the tables in sync by continuously streaming changes.

[Read the Blog post on Podyn](https://www.citusdata.com/blog/2017/09/22/dynamodb-to-postgres-replication/).

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

./podyn --postgres-jdbc-url "jdbc:postgresql://host:5432/citus?sslmode=require&user=citus&password=pw" --schema --data --citus

Constructing table schema for table clicks
Moving data for table clicks
Adding new column to table clicks: ip text
Adding new column to table clicks: object text
```

When `--schema` is specified, tables will be created in PostgreSQL as described in the *Schema conversion rules* section. If `--citus` is specified the tables will be distributed by the DynamoDB partition key. When the `--data` argument is specified, all the data in the DynamoDB table is scanned in batches and `COPY` is used to load the batch into postgres.

## Stream changes from DynamoDB

After schema creation and the initial data load, you can continuously stream changes using:

```
./podyn --postgres-jdbc-url "jdbc:postgresql://host:5432/citus?sslmode=require&user=citus&password=pw" --changes --citus

Replicating changes for table clicks
...
```

The changes are processed in batches and new fields are added to the table as columns. The changes are translated into delete  or upsert statements that are sent to postgres over multiple connections (specified using `-n`) to achieve high throughput.

When running the command immediately after a data load, some changes that were made prior to the data load may be re-applied, causing the replicated database to temporarily regress. However, since the changes are applied in the same order they will eventually arrive at the current value. After loading a batch of changes into the database, a checkpoint is made. If the tool is restarted, it will continue from its last checkpoint. The checkpoints are stored in DynamoDB tables prefixed with `podyn_migration_`. 

## Schema conversion rules

Podyn currently has two ways of converting a DynamoDB schema to a PostgreSQL schema: `columns` and `jsonb`. The default is `columns`. To use the `jsonb` conversion mode add `-m jsonb` as an argument.

### Columns mode

In the default schema conversion mode (`-m columns`), top-level keys in DynamoDB items are translated into columns. The initial schema is derived from the partition key, sort key and secondary indexes. When using Citus, the primary partition key becomes the distribution column.

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
   PRIMARY KEY (sitename, "time")
);
CREATE INDEX "pageid-index" ON clicks (pageid);
SELECT create_distributed_table('clicks', 'sitename');
```

A new column is added to the PostgreSQL table whenever a new key appears in an item during the initial data load or while replicating changes. For example, if `"ip" -> "198.51.100.3"` key is encountered in an item and the table does not have an `ip` column, then the following command is sent to the database:

```
ALTER TABLE clicks ADD COLUMN ip text;
```

After replicating a single item, the table might look as follows.

```
SELECT * FROM clicks;
-[ RECORD 1 ]-------------------------------------------------------------------------------------------------------------------------------------
sitename | citusdata.com
time     | 2017-09-18 16:08:36.989788+02
pageid   | 347712
ip       | 198.51.100.3
object   | home_button
```

In DynamoDB, the same key can appear with different types as long as it's not part of the primary key or a secondary index. If Podyn encounters a key-value pair with a type that's not compatible with the column type, then a new column is added with the type name added as a suffix. For example, if an item appears which contains `"ip" -> false`, then Podyn would send the following command to PostgreSQL:

```
ALTER TABLE clicks ADD COLUMN ip_boolean boolean;
```

If you have keys that have values of different types, or many different top-level keys, then you may want to consider using JSONB mode.

### JSONB mode

In the JSONB conversion mode (`-m jsonb`), the initial schema is never changed. Instead, a single JSONB column is added that  contains the entire DynamoDB item in JSON format.

Constructing the schema in `jsonb` mode is done in the same way as in `columns` mode (partition key and secondary indexes become columns) except with an extra column named `data` with the `jsonb` type. In the `clicks` example, Podyn would send the following statements to PostgreSQL:

```
CREATE TABLE clicks (
   sitename text NOT NULL,
   "time" text NOT NULL,
   pageid numeric NOT NULL,
   data jsonb,
   PRIMARY KEY (sitename, "time")
);
CREATE INDEX "pageid-index" ON clicks (pageid);
SELECT create_distributed_table('clicks', 'sitename');
```

During replication, the entire DynamoDB item is converted to JSON, including the fields that already appear as columns, for example:

```
SELECT * FROM clicks;
-[ RECORD 1 ]-------------------------------------------------------------------------------------------------------------------------------------
sitename | citusdata.com
time     | 2017-09-18 16:08:36.989788+02
pageid   | 347712
data     | {"ip": "198.51.100.3", "time": "2017-09-18 16:08:36.989788+02", "object": "home_button", "pageid": 347712, "sitename": "citusdata.com"}
```

The JSONB mode has an additional advantage that you can safely set up multiple Podyn instances for replicating changes. They will automatically divide the work and if a node fails, the other node(s) will automatically take over.
