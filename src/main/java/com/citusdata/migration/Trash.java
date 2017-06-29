package com.citusdata.migration;

import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.Shard;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.citusdata.migration.datamodel.TableColumn;
import com.citusdata.migration.datamodel.TableColumnType;
import com.citusdata.migration.datamodel.TableColumnValue;
import com.citusdata.migration.datamodel.TableEmitter;
import com.citusdata.migration.datamodel.TableRow;
import com.citusdata.migration.datamodel.TableSchema;

public class Trash {
	
	String tableName;
	TableEmitter emitter;
	AmazonDynamoDB dynamoDBClient;
	AmazonDynamoDBStreams streamsClient;
	DynamoDB dynamoDB;

	public Trash() {
		// TODO Auto-generated constructor stub
	}

	public void copyData3(TableSchema tableSchema) throws SQLException, IOException {
		Reader scan = scanTable(tableSchema);
		//emitter.copyFromReader(scan);
	}

	public void copyData2(TableSchema tableSchema) {
		Table table = dynamoDB.getTable(tableName);

		ScanSpec scanSpec = new ScanSpec().withConsistentRead(true);

		ItemCollection<ScanOutcome> result = table.scan(scanSpec);
		for (Item item : result) {
			TableRow tableRow = tableSchema.createRow();

			for (String columnName : tableSchema.getColumnNames()) {
				if (item.hasAttribute(columnName)) {
					tableRow.setValue(columnName, item.getString(columnName));
					item.removeAttribute(columnName);
				} 
			}

			tableRow.setValue("data", item.toJSON());

			System.out.println(tableRow.toUpsert() + ";");
		}
	}

	public Reader scanTable(TableSchema tableSchema) {
		final Table table = dynamoDB.getTable(tableName);
		final ScanSpec scanSpec = new ScanSpec().withConsistentRead(true);
		final ItemCollection<ScanOutcome> result = table.scan(scanSpec);
		final IteratorSupport<Item, ScanOutcome> iterator = result.iterator();

		return new Reader() {

			String currentRow = "";
			int currentOffset = 0;

			@Override
			public int read(char[] cbuf, int off, int len) throws IOException {
				int numBytesCopiedTotal = 0;

				while(numBytesCopiedTotal < len) {
					if (currentOffset >= currentRow.length()) {
						if(!iterator.hasNext()) {
							break;
						}

						currentRow = getRow();
						currentOffset = 0;
					}

					int numBytesAllowed = len - numBytesCopiedTotal;
					int endIndex = Math.min(currentRow.length(), currentOffset + numBytesAllowed);
					int numBytesFromString = endIndex - currentOffset;

					for(int i = 0; i < numBytesFromString; i++) {
						cbuf[numBytesCopiedTotal + off + i] = currentRow.charAt(currentOffset + i);
					}

					currentOffset = endIndex;
					numBytesCopiedTotal += numBytesFromString;
				}

				if (numBytesCopiedTotal == 0 && len > 0) {
					return -1;
				}

				return numBytesCopiedTotal;
			}

			public String getRow() {
				Item item = iterator.next();
				TableRow tableRow = tableSchema.createRow();

				for (String columnName : tableSchema.getColumnNames()) {
					if (item.hasAttribute(columnName)) {
						tableRow.setValue(columnName, item.getString(columnName));
						item.removeAttribute(columnName);
					}
				}

				tableRow.setValue("data", item.toJSON());

				return tableRow.toCopyRow() + '\n';
			}

			@Override
			public void close() throws IOException {

			}
		};
	}

	public void copyData1(TableSchema tableSchema) {
		ScanRequest scanRequest = new ScanRequest().withTableName(tableName).withConsistentRead(true);

		ScanResult result = dynamoDBClient.scan(scanRequest);

		for (Map<String, AttributeValue> item : result.getItems()) {
			TableRow tableRow = tableSchema.createRow();
			Item payloadItem = new Item();

			for(Map.Entry<String, AttributeValue> entry : item.entrySet()) {
				AttributeValue value = entry.getValue();

				if (tableSchema.columnExists(entry.getKey())) {
					tableRow.setValue(entry.getKey(), value.getS());
				} else {
					payloadItem.withString(entry.getKey(), value.getS());
				}
			}

			tableRow.setValue("data", payloadItem.toJSON());

			System.out.println(tableRow.toUpsert() + ";");
		}
	}

	public void copyChanges1(TableSchema tableSchema) throws SQLException {
		DescribeTableResult describeTableResult = dynamoDBClient.describeTable(tableName);
		TableDescription tableDescription = describeTableResult.getTable();
		String tableStreamArn = tableDescription.getLatestStreamArn();

		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamArn(tableStreamArn);
		DescribeStreamResult describeStreamResult = streamsClient.describeStream(describeStreamRequest);
		List<Shard> shards = describeStreamResult.getStreamDescription().getShards();

		for (Shard shard : shards) {
			String shardId = shard.getShardId();

			// Get an iterator for the current shard

			GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest().
					withStreamArn(tableStreamArn).
					withShardId(shardId).
					withShardIteratorType(ShardIteratorType.TRIM_HORIZON);
			GetShardIteratorResult getShardIteratorResult = streamsClient.getShardIterator(getShardIteratorRequest);
			String nextItr = getShardIteratorResult.getShardIterator();

			while (nextItr != null) {

				// Use the iterator to read the data records from the shard

				GetRecordsRequest getRecordsRequest = new GetRecordsRequest().
						withShardIterator(nextItr);
				
				GetRecordsResult getRecordsResult = streamsClient
						.getRecords(getRecordsRequest);
				List<Record> records = getRecordsResult.getRecords();

				for (Record record : records) {
					StreamRecord dynamoRecord = record.getDynamodb();

					if (!"REMOVE".equals(record.getEventName())) {
						Map<String,AttributeValue> values = dynamoRecord.getNewImage();
						
						for(Map.Entry<String,AttributeValue> entry : values.entrySet()) {
							String columnName = entry.getKey();
							
							if (!tableSchema.columnExists(columnName)) {
								TableColumnType columnType = DynamoDBTableReplicator.columnTypeFromDynamoValue(entry.getValue());
								TableColumn newColumn = tableSchema.addColumn(columnName, columnType);
								emitter.createColumn(newColumn);
							}
						}
					}
				}
				
				for (Record record : records) {
					StreamRecord dynamoRecord = record.getDynamodb();

					if ("REMOVE".equals(record.getEventName())) {
						Map<String,AttributeValue> dynamoKeys = dynamoRecord.getKeys();
						Map<String,TableColumnValue> primaryKeyValues = new HashMap<>();

						for(Map.Entry<String,AttributeValue> entry : dynamoKeys.entrySet()) {
							String columnName = entry.getKey();

							if (!tableSchema.isInPrimaryKey(columnName)) {
								continue;
							}
							
							TableColumn column = tableSchema.getColumn(columnName);
							TableColumnValue columnValue = DynamoDBTableReplicator.columnValueFromDynamoValue(entry.getValue());

							primaryKeyValues.put(columnName, columnValue);
						}

						//emitter.delete(primaryKeyValues);

					} else {
						Map<String,AttributeValue> dynamoItem = dynamoRecord.getNewImage();
						TableRow tableRow = DynamoDBTableReplicator.rowFromDynamoRecord(tableSchema, dynamoItem);

						emitter.upsert(tableRow);
					}
				}
				
				nextItr = getRecordsResult.getNextShardIterator();
			}

		}
	}
}
