/**
 * 
 */
package com.citusdata.migration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.citusdata.migration.datamodel.Delete;
import com.citusdata.migration.datamodel.PrimaryKeyValue;
import com.citusdata.migration.datamodel.TableColumn;
import com.citusdata.migration.datamodel.TableColumnType;
import com.citusdata.migration.datamodel.TableColumnValue;
import com.citusdata.migration.datamodel.TableModification;
import com.citusdata.migration.datamodel.TableRow;
import com.citusdata.migration.datamodel.TableSchema;
import com.citusdata.migration.datamodel.Upsert;

/**
 * @author marco
 *
 */
public class DynamoDBShardStreamer {

	final AmazonDynamoDBStreams streamsClient;
	public final String shardId;

	private String nextShardIterator;

	public DynamoDBShardStreamer(AmazonDynamoDBStreams streamsClient, String tableStreamArn, String shardId) {
		this.streamsClient = streamsClient;
		this.shardId = shardId;

		final GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest().
				withStreamArn(tableStreamArn).
				withShardId(shardId).
				withShardIteratorType(ShardIteratorType.TRIM_HORIZON);
		final GetShardIteratorResult getShardIteratorResult = streamsClient.getShardIterator(getShardIteratorRequest);
		nextShardIterator = getShardIteratorResult.getShardIterator();
	}

	public StreamedBatch scan() {
		final GetRecordsRequest getRecordsRequest = new GetRecordsRequest().
				withShardIterator(nextShardIterator);
		final GetRecordsResult getRecordsResult = streamsClient.getRecords(getRecordsRequest);
		final List<Record> records = getRecordsResult.getRecords();

		this.nextShardIterator = getRecordsResult.getNextShardIterator();

		return new StreamedBatch() {

			@Override
			public List<TableColumn> addNewColumns(TableSchema tableSchema) {
				Set<TableColumn> newColumns = new HashSet<>();

				for (Record record : records) {
					StreamRecord dynamoRecord = record.getDynamodb();
					Map<String,AttributeValue> values = dynamoRecord.getNewImage();

					if (values == null) {
						continue;
					}

					for(Map.Entry<String,AttributeValue> entry : values.entrySet()) {
						String columnName = entry.getKey();

						if (!tableSchema.columnExists(columnName)) {
							TableColumnType columnType = DynamoDBScanner.columnTypeFromDynamoValue(entry.getValue());
							TableColumn newColumn = tableSchema.addColumn(columnName, columnType);
							newColumns.add(newColumn);
						}
					}
				}

				return new ArrayList<>(newColumns);
			}

			@Override
			public List<TableModification> modifications(TableSchema tableSchema) {
				List<TableModification> modifications = new ArrayList<>();

				for (Record record : records) {
					StreamRecord dynamoRecord = record.getDynamodb();

					if ("REMOVE".equals(record.getEventName())) {
						Map<String,AttributeValue> dynamoKeys = dynamoRecord.getKeys();
						PrimaryKeyValue keyValue = new PrimaryKeyValue(tableSchema);

						for(Map.Entry<String,AttributeValue> entry : dynamoKeys.entrySet()) {
							String columnName = entry.getKey();

							if (!tableSchema.isInPrimaryKey(columnName)) {
								continue;
							}

							TableColumnValue columnValue = DynamoDBScanner.columnValueFromDynamoValue(entry.getValue());

							keyValue.setValue(columnName, columnValue);
						}

						modifications.add(new Delete(keyValue));

					} else {
						Map<String,AttributeValue> dynamoItem = dynamoRecord.getNewImage();
						TableRow tableRow = DynamoDBScanner.rowFromDynamoRecord(tableSchema, dynamoItem);

						modifications.add(new Upsert(tableRow));
					}
				}

				return modifications;
			}

			@Override
			public boolean hasNextBatch() {
				return getRecordsResult.getNextShardIterator() != null;
			}
		};


	}

}
