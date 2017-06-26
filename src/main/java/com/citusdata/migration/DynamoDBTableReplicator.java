/**
 * 
 */
package com.citusdata.migration;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Shard;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.citusdata.migration.datamodel.Delete;
import com.citusdata.migration.datamodel.TableColumn;
import com.citusdata.migration.datamodel.TableColumnType;
import com.citusdata.migration.datamodel.TableEmitter;
import com.citusdata.migration.datamodel.TableModification;
import com.citusdata.migration.datamodel.TableRowBatch;
import com.citusdata.migration.datamodel.TableSchema;
import com.citusdata.migration.datamodel.Upsert;
import com.google.common.util.concurrent.RateLimiter;

/**
 * @author marco
 *
 */
public class DynamoDBTableReplicator {

	final AmazonDynamoDBStreams streamsClient;
	final AmazonDynamoDB dynamoDBClient;

	final TableEmitter emitter;
	final String dynamoTableName;

	TableSchema tableSchema;
	ExecutorService executor;

	public DynamoDBTableReplicator(
			AmazonDynamoDB dynamoDBClient,
			AmazonDynamoDBStreams streamsClient, 
			TableEmitter emitter,
			String tableName) {
		this.dynamoDBClient = dynamoDBClient;
		this.streamsClient = streamsClient;
		this.emitter = emitter;
		this.dynamoTableName = tableName;
	}

	DynamoDBScanner createScanner() {
		return new DynamoDBScanner(dynamoDBClient, dynamoTableName);
	}

	DynamoDBShardStreamer createStreamer(String tableStreamArn, String shardId) {
		return new DynamoDBShardStreamer(streamsClient, tableStreamArn, shardId);
	}

	public void replicateSchema() {
		try {
			tableSchema = fetchBaseSchema();
			emitter.createTable(tableSchema);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}

	TableSchema fetchBaseSchema() {
		TableSchema tableSchema = new TableSchema(dynamoTableName);

		DescribeTableResult describeTableResult = dynamoDBClient.describeTable(dynamoTableName);
		TableDescription tableDescription = describeTableResult.getTable();

		List<AttributeDefinition> attributeDefinitions = tableDescription.getAttributeDefinitions();

		for (AttributeDefinition attributeDefinition : attributeDefinitions) {
			String keyName = attributeDefinition.getAttributeName();
			TableColumnType type = TableColumnType.text;

			switch(attributeDefinition.getAttributeType()) {
			case "N":
				type = TableColumnType.numeric;
			case "B":
				type = TableColumnType.bytea;
			}

			tableSchema.addColumn(keyName, type);
		}

		List<String> primaryKey = new ArrayList<>();
		List<KeySchemaElement> keySchema = tableDescription.getKeySchema();

		for (KeySchemaElement keySchemaElement : keySchema) {
			String keyName = keySchemaElement.getAttributeName();
			String keyType = keySchemaElement.getKeyType();

			TableColumn column = tableSchema.getColumn(keyName);

			if (KeyType.fromValue(keyType) == KeyType.HASH) {
				tableSchema.setDistributionColumn(keyName);
			}

			column.notNull = true;

			primaryKey.add(keyName);
		}

		tableSchema.setPrimaryKey(primaryKey);

		List<GlobalSecondaryIndexDescription> secondaryIndexes = tableDescription.getGlobalSecondaryIndexes();

		for (GlobalSecondaryIndexDescription secondaryIndex : secondaryIndexes) {
			String indexName = secondaryIndex.getIndexName();
			List<String> indexColumns = new ArrayList<>();

			for (KeySchemaElement keySchemaElement : secondaryIndex.getKeySchema()) {
				String keyName = keySchemaElement.getAttributeName();

				indexColumns.add(keyName);
			}

			tableSchema.addIndex(indexName, indexColumns);
		}

		return tableSchema;
	}

	public void replicateData(int maxScanRate) {
		try {
			DynamoDBScanner scanner = createScanner();
			RateLimiter rateLimiter = RateLimiter.create(maxScanRate);

			while(true) {
				ScannedBatch resultBatch = scanner.scan();

				List<TableColumn> newColumns = resultBatch.addNewColumnsToSchema(tableSchema);

				/* create new columns in the destination */
				for(TableColumn newColumn : newColumns) {
					emitter.createColumn(newColumn);
				}

				TableRowBatch tableRowBatch = resultBatch.rows(tableSchema);

				/* load the batch using COPY */
				emitter.copyFromReader(tableSchema, tableRowBatch.asCopyReader());

				if(!resultBatch.hasNextBatch()) {
					break;
				}

				// Account for the rest of the throughput we consumed, 
				// now that we know how much that scan request cost 
				double consumedCapacity = resultBatch.getConsumedCapacity();
				int permitsToConsume = (int)(consumedCapacity - 1.0);
				if (permitsToConsume <= 0) {
					permitsToConsume = 1;
				}

				// Let the rate limiter wait until our desired throughput "recharges"
				rateLimiter.acquire(permitsToConsume);
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}

	public void replicateChanges() throws SQLException {

		DescribeTableResult describeTableResult = dynamoDBClient.describeTable(dynamoTableName);
		TableDescription tableDescription = describeTableResult.getTable();
		String tableStreamArn = tableDescription.getLatestStreamArn();

		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamArn(tableStreamArn);
		DescribeStreamResult describeStreamResult = streamsClient.describeStream(describeStreamRequest);
		List<Shard> shards = describeStreamResult.getStreamDescription().getShards();

		executor = Executors.newFixedThreadPool(shards.size());

		for (Shard shard : shards) {
			String shardId = shard.getShardId();

			final DynamoDBShardStreamer replicator = createStreamer(tableStreamArn, shardId);

			// TODO: better error handling
			executor.execute(new Runnable() {
				public void run() {
					replicateChanges(replicator);
				}
			});
		}

		executor.shutdown();
	}

	void replicateChanges(DynamoDBShardStreamer replicator) {
		try {
			while(true) {
				StreamedBatch resultBatch = replicator.scan();

				List<TableColumn> newColumns = resultBatch.addNewColumns(tableSchema);

				for(TableColumn newColumn : newColumns) {
					emitter.createColumn(newColumn);
				}

				List<TableModification> modifications = resultBatch.modifications(tableSchema);

				for(TableModification modification : modifications) {
					if (modification instanceof Delete) {
						Delete delete = (Delete) modification;
						emitter.delete(delete.getKeyValue());
					} else if (modification instanceof Upsert) {
						Upsert upsert = (Upsert) modification;
						emitter.upsert(upsert.getNewRow());
					}
				}

				if (!resultBatch.hasNextBatch()) {
					break;
				}

				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
				}
			}
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}



}
