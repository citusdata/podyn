/**
 * 
 */
package com.citusdata.migration;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.internal.InternalUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.util.json.Jackson;
import com.citusdata.migration.datamodel.PrimaryKeyValue;
import com.citusdata.migration.datamodel.TableColumn;
import com.citusdata.migration.datamodel.TableColumnType;
import com.citusdata.migration.datamodel.TableColumnValue;
import com.citusdata.migration.datamodel.TableEmitter;
import com.citusdata.migration.datamodel.TableExistsException;
import com.citusdata.migration.datamodel.TableRow;
import com.citusdata.migration.datamodel.TableRowBatch;
import com.citusdata.migration.datamodel.TableSchema;
import com.google.common.util.concurrent.RateLimiter;

/**
 * @author marco
 *
 */
public class DynamoDBTableReplicator {

	private static final Log LOG = LogFactory.getLog(DynamoDBTableReplicator.class);

	public static final String APPLICATION_NAME = "dynamodb-to-postgres";
	public static final String LEASE_TABLE_PREFIX = "d2p_migration_";

	final AmazonDynamoDBStreams streamsClient;
	final AmazonDynamoDB dynamoDBClient;
	final AWSCredentialsProvider awsCredentialsProvider;
	final ExecutorService executor;

	final TableEmitter emitter;
	final String dynamoTableName;

	boolean addColumnsEnabled;
	boolean useCitus;
	boolean useLowerCaseColumnNames;
	ConversionMode conversionMode;

	TableSchema tableSchema;

	public DynamoDBTableReplicator(
			AmazonDynamoDB dynamoDBClient,
			AmazonDynamoDBStreams streamsClient,
			AWSCredentialsProvider awsCredentialsProvider,
			ExecutorService executorService,
			TableEmitter emitter,
			String tableName) throws SQLException {
		this.dynamoDBClient = dynamoDBClient;
		this.streamsClient = streamsClient;
		this.awsCredentialsProvider = awsCredentialsProvider;
		this.executor = executorService;
		this.emitter = emitter;
		this.dynamoTableName = tableName;
		this.addColumnsEnabled = true;
		this.useCitus = false;
		this.useLowerCaseColumnNames = false;
		this.tableSchema = emitter.fetchSchema(this.dynamoTableName);
	}

	public void setUseCitus(boolean useCitus) {
		this.useCitus = useCitus;
	}

	public void setAddColumnEnabled(boolean addColumnEnabled) {
		this.addColumnsEnabled = addColumnEnabled;
	}

	public void setUseLowerCaseColumnNames(boolean useLowerCaseColumnNames) {
		this.useLowerCaseColumnNames = useLowerCaseColumnNames;
	}

	public void setConversionMode(ConversionMode conversionMode) {
		this.conversionMode = conversionMode;
	}

	String dynamoKeyToColumnName(String keyName) {
		if (useLowerCaseColumnNames) {
			return keyName.toLowerCase();
		} else {
			return keyName;
		}
	}

	public void replicateSchema() throws TableExistsException {
		if (tableSchema != null) {
			throw new TableExistsException("relation %s already exists", dynamoTableName);
		}

		tableSchema = fetchSourceSchema();
		emitter.createTable(tableSchema);
	}

	TableSchema fetchSourceSchema() {
		TableSchema tableSchema = new TableSchema(dynamoTableName);

		DescribeTableResult describeTableResult = dynamoDBClient.describeTable(dynamoTableName);
		TableDescription tableDescription = describeTableResult.getTable();

		List<AttributeDefinition> attributeDefinitions = tableDescription.getAttributeDefinitions();

		for (AttributeDefinition attributeDefinition : attributeDefinitions) {
			String keyName = attributeDefinition.getAttributeName();
			String columnName = dynamoKeyToColumnName(keyName);
			TableColumnType type = TableColumnType.text;

			switch(attributeDefinition.getAttributeType()) {
			case "N":
				type = TableColumnType.numeric;
				break;
			case "B":
				type = TableColumnType.bytea;
				break;
			}

			tableSchema.addColumn(columnName, type);
		}

		List<String> primaryKey = new ArrayList<>();
		List<KeySchemaElement> keySchema = tableDescription.getKeySchema();

		for (KeySchemaElement keySchemaElement : keySchema) {
			String keyName = keySchemaElement.getAttributeName();
			String keyType = keySchemaElement.getKeyType();
			String columnName = dynamoKeyToColumnName(keyName);

			TableColumn column = tableSchema.getColumn(columnName);

			if (useCitus && KeyType.fromValue(keyType) == KeyType.HASH) {
				tableSchema.setDistributionColumn(columnName);
			}

			column.notNull = true;

			primaryKey.add(columnName);
		}

		tableSchema.setPrimaryKey(primaryKey);

		List<GlobalSecondaryIndexDescription> secondaryIndexes = tableDescription.getGlobalSecondaryIndexes();

		if (secondaryIndexes != null) {
			for (GlobalSecondaryIndexDescription secondaryIndex : secondaryIndexes) {
				String indexName = secondaryIndex.getIndexName();
				List<String> indexColumns = new ArrayList<>();

				for (KeySchemaElement keySchemaElement : secondaryIndex.getKeySchema()) {
					String keyName = keySchemaElement.getAttributeName();
					String columnName = dynamoKeyToColumnName(keyName);

					indexColumns.add(columnName);
				}

				tableSchema.addIndex(indexName, indexColumns);
			}
		}

		if(conversionMode == ConversionMode.jsonb) {
			tableSchema.addColumn("data", TableColumnType.jsonb);
		}

		return tableSchema;
	}

	public Future<Long> startReplicatingData(final int maxScanRate) {
		return executor.submit(new Callable<Long>() {
			@Override
			public Long call() throws Exception {
				return replicateData(maxScanRate);
			}
		});
	}

	public long replicateData(int maxScanRate) {
		RateLimiter rateLimiter = RateLimiter.create(maxScanRate);

		Map<String,AttributeValue> lastEvaluatedScanKey = null;
		long numRowsReplicated = 0;

		while(true) {
			ScanResult scanResult = scanWithRetries(lastEvaluatedScanKey);

			if (addColumnsEnabled) {
				for(Map<String,AttributeValue> dynamoItem : scanResult.getItems()) {
					addNewColumns(dynamoItem);
				}
			}

			TableRowBatch tableRowBatch = new TableRowBatch();

			for(Map<String,AttributeValue> dynamoItem : scanResult.getItems()) {
				TableRow tableRow = rowFromDynamoRecord(dynamoItem);

				tableRowBatch.addRow(tableRow);
			}

			LOG.info(String.format("Replicated %d rows to table %s", tableRowBatch.size(), tableSchema.tableName));

			numRowsReplicated += tableRowBatch.size();

			/* load the batch using COPY */
			emitter.copyFromReader(tableSchema, tableRowBatch.asCopyReader());

			lastEvaluatedScanKey = scanResult.getLastEvaluatedKey();

			if(lastEvaluatedScanKey == null) {
				break;
			}

			// Account for the rest of the throughput we consumed, 
			// now that we know how much that scan request cost 
			double consumedCapacity = scanResult.getConsumedCapacity().getCapacityUnits();
			int permitsToConsume = (int)(consumedCapacity - 1.0);
			if (permitsToConsume <= 0) {
				permitsToConsume = 1;
			}

			// Let the rate limiter wait until our desired throughput "recharges"
			rateLimiter.acquire(permitsToConsume);
		}

		return numRowsReplicated;
	}

	private ScanResult scanWithRetries(Map<String, AttributeValue> lastEvaluatedScanKey) {
		ScanRequest scanRequest = new ScanRequest().
				withTableName(this.dynamoTableName).
				withConsistentRead(true).
				withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL).
				withLimit(100).
				withExclusiveStartKey(lastEvaluatedScanKey);

		for (int tryNumber = 1; ; tryNumber++) {
			try {
				ScanResult scanResult = dynamoDBClient.scan(scanRequest);
				return scanResult;
			} catch (ProvisionedThroughputExceededException e) {
				if (tryNumber == 3) {
					throw e;
				}
			} catch (InternalServerErrorException e) {
				if (tryNumber == 3) {
					throw e;
				}
			}

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	public String getStreamArn() {
		DescribeTableResult describeTableResult = dynamoDBClient.describeTable(dynamoTableName);
		TableDescription tableDescription = describeTableResult.getTable();
		String tableStreamArn = tableDescription.getLatestStreamArn();
		return tableStreamArn;
	}


	public void startReplicatingChanges() throws StreamNotEnabledException {
		if (tableSchema == null) {
			throw new TableExistsException("table %s does not exist in destination", dynamoTableName);
		}

		String tableStreamArn = getStreamArn();

		if (tableStreamArn == null) {
			throw new StreamNotEnabledException("table %s does not have a stream enabled\n", dynamoTableName);
		}

		AmazonDynamoDBStreamsAdapterClient adapterClient = new AmazonDynamoDBStreamsAdapterClient(streamsClient);
		AmazonCloudWatch cloudWatchClient = AmazonCloudWatchClientBuilder.standard().build();

		String workerId = generateWorkerId();

		final KinesisClientLibConfiguration workerConfig = new KinesisClientLibConfiguration(
				APPLICATION_NAME, tableStreamArn, awsCredentialsProvider, workerId).
				withMaxRecords(1000).
				withIdleTimeBetweenReadsInMillis(500).
				withCallProcessRecordsEvenForEmptyRecordList(false).
				withCleanupLeasesUponShardCompletion(false).
				withFailoverTimeMillis(20000).
				withTableName("d2p_migration_" + dynamoTableName).
				withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

		Worker worker = new Worker.Builder().
				recordProcessorFactory(recordProcessorFactory).
				config(workerConfig).
				kinesisClient(adapterClient).
				cloudWatchClient(cloudWatchClient).
				dynamoDBClient(dynamoDBClient).
				execService(executor).
				build();

		executor.execute(worker);
	}

	IRecordProcessorFactory recordProcessorFactory = new IRecordProcessorFactory() {
		@Override
		public IRecordProcessor createProcessor() {
			return createStreamProcessor();
		}
	};

	protected IRecordProcessor createStreamProcessor() {
		return new IRecordProcessor() {

			@Override
			public void initialize(InitializationInput initializationInput) {
			}

			public List<Record> extractDynamoStreamRecords(List<com.amazonaws.services.kinesis.model.Record> kinesisRecords) {
				List<Record> dynamoRecords = new ArrayList<>(kinesisRecords.size());

				for(com.amazonaws.services.kinesis.model.Record kinesisRecord : kinesisRecords) {
					if (kinesisRecord instanceof RecordAdapter) {
						Record dynamoRecord = ((RecordAdapter) kinesisRecord).getInternalObject();
						dynamoRecords.add(dynamoRecord);
					}
				}

				return dynamoRecords;
			}

			@Override
			public void processRecords(ProcessRecordsInput processRecordsInput) {
				List<Record> records = extractDynamoStreamRecords(processRecordsInput.getRecords());

				DynamoDBTableReplicator.this.processRecords(records);

				checkpoint(processRecordsInput.getCheckpointer());
			}

			@Override
			public void shutdown(ShutdownInput shutdownInput) {
				if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
					checkpoint(shutdownInput.getCheckpointer());
				}
			}

			void checkpoint(IRecordProcessorCheckpointer checkpointer) {
				try {
					checkpointer.checkpoint();
				} catch (KinesisClientLibDependencyException|InvalidStateException|ThrottlingException|ShutdownException e) {
					LOG.warn(e);
				}
			}
		};
	}

	void processRecords(List<Record> records) {
		if (addColumnsEnabled) {
			for (Record dynamoRecord : records) {
				StreamRecord streamRecord = dynamoRecord.getDynamodb();
				Map<String,AttributeValue> item = streamRecord.getNewImage();

				if (item == null) {
					continue;
				}

				addNewColumns(item);
			}
		}

		for (Record dynamoRecord : records) {
			StreamRecord streamRecord = dynamoRecord.getDynamodb();

			switch (dynamoRecord.getEventName()) {
			case "INSERT":
			case "MODIFY":
				Map<String,AttributeValue> dynamoItem = streamRecord.getNewImage();

				if(dynamoItem == null) {
					LOG.error(String.format("the stream for table %s does not have new images", dynamoTableName));
					System.exit(1);
				}

				TableRow tableRow = rowFromDynamoRecord(dynamoItem);
				emitter.upsert(tableRow);
				LOG.debug(tableRow.toUpsert());
				break;
			case "REMOVE":
				Map<String,AttributeValue> dynamoKeys = streamRecord.getKeys();
				PrimaryKeyValue keyValue = primaryKeyValueFromDynamoKeys(dynamoKeys);
				emitter.delete(keyValue);
				LOG.debug(keyValue.toDelete());
				break;
			}

			LOG.debug(streamRecord);
		}

		LOG.info(String.format("Replicated %d changes to table %s", records.size(), tableSchema.tableName));
	}

	void addNewColumns(Map<String,AttributeValue> item) {
		if(conversionMode == ConversionMode.jsonb) {
			/* don't add new columns in jsonb mode */
			return;
		}

		for(Map.Entry<String,AttributeValue> entry : item.entrySet()) {
			String keyName = entry.getKey();
			String columnName = dynamoKeyToColumnName(keyName);
			TableColumn column = tableSchema.getColumn(columnName);
			TableColumnType valueType = DynamoDBTableReplicator.columnTypeFromDynamoValue(entry.getValue());

			if (column == null) {
				column = tableSchema.addColumn(columnName, valueType);
				LOG.info(String.format("Adding new column to table %s: %s", tableSchema.tableName, column));
				emitter.createColumn(column);
			} else if (column.type != valueType) {
				columnName = columnName + "_" + valueType;
				column = tableSchema.getColumn(columnName);

				if (column == null) {
					column = tableSchema.addColumn(columnName, valueType);
					LOG.info(String.format("Adding new column to table %s: %s", tableSchema.tableName, column));
					emitter.createColumn(column);
				}
			}
		}
	}

	PrimaryKeyValue primaryKeyValueFromDynamoKeys(Map<String,AttributeValue> dynamoKeys) {
		PrimaryKeyValue keyValue = new PrimaryKeyValue(tableSchema);

		for(Map.Entry<String,AttributeValue> entry : dynamoKeys.entrySet()) {
			String keyName = entry.getKey();
			String columnName = dynamoKeyToColumnName(keyName);

			if (!tableSchema.isInPrimaryKey(columnName)) {
				continue;
			}

			TableColumnValue columnValue = DynamoDBTableReplicator.columnValueFromDynamoValue(entry.getValue());

			keyValue.setValue(columnName, columnValue);
		}

		return keyValue;
	}

	static String generateWorkerId() {
		StringBuilder sb = new StringBuilder();

		try {
			sb.append(InetAddress.getLocalHost().getCanonicalHostName());
		} catch (UnknownHostException e) {
		}

		sb.append('_');
		sb.append(UUID.randomUUID());

		return sb.toString();
	}

	public TableRow rowFromDynamoRecord(Map<String,AttributeValue> dynamoItem) {
		if (conversionMode == ConversionMode.jsonb) {
			return rowWithJsonbFromDynamoRecord(dynamoItem);
		} else {
			return rowWithColumnsFromDynamoRecord(dynamoItem);

		}
	}

	public TableRow rowWithJsonbFromDynamoRecord(Map<String,AttributeValue> dynamoItem) {
		TableRow row = tableSchema.createRow();
		Item item = new Item();

		for(Map.Entry<String, AttributeValue> entry : dynamoItem.entrySet()) {
			String keyName = entry.getKey();
			String columnName = dynamoKeyToColumnName(keyName);
			TableColumn column = tableSchema.getColumn(columnName);
			AttributeValue typedValue = entry.getValue();
			TableColumnValue columnValue = columnValueFromDynamoValue(typedValue);

			if (column != null) {
				row.setValue(columnName, columnValue);
			}

			item.with(keyName, columnValue.datum);
		}

		row.setValue("data", item.toJSON());

		return row;
	}

	public TableRow rowWithColumnsFromDynamoRecord(Map<String,AttributeValue> dynamoItem) {
		TableRow row = tableSchema.createRow();

		for(Map.Entry<String, AttributeValue> entry : dynamoItem.entrySet()) {
			String keyName = entry.getKey();
			String columnName = dynamoKeyToColumnName(keyName);
			TableColumn column = tableSchema.getColumn(columnName);

			if (column == null) {
				/* skip non-existent columns */
				continue;
			}

			AttributeValue typedValue = entry.getValue();
			TableColumnValue columnValue = columnValueFromDynamoValue(typedValue);

			if (columnValue.type == column.type) {
				row.setValue(columnName, columnValue);
			} else {

				row.setValue(columnName + "_" + columnValue.type, columnValue);
			}
		}

		return row;
	}

	public static TableColumnValue columnValueFromDynamoValue(AttributeValue typedValue) {
		if(typedValue.getB() != null) {
			ByteBuffer value = typedValue.getB();
			return new TableColumnValue(TableColumnType.bytea, value.array());
		} else if (typedValue.getBOOL() != null) {
			Boolean value = typedValue.getBOOL();
			return new TableColumnValue(TableColumnType.bool, value);
		} else if (typedValue.getBS() != null) {
			List<ByteBuffer> value = typedValue.getBS();
			return new TableColumnValue(TableColumnType.jsonb, Jackson.toJsonString(value));
		} else if (typedValue.getL() != null) {
			List<AttributeValue> value = typedValue.getL();
			List<Object> simpleList = InternalUtils.toSimpleList(value);
			return new TableColumnValue(TableColumnType.jsonb, Jackson.toJsonString(simpleList));
		} else if (typedValue.getM() != null) {
			Map<String,AttributeValue> value = typedValue.getM();
			Item simpleMap = Item.fromMap(InternalUtils.toSimpleMapValue(value));
			return new TableColumnValue(TableColumnType.jsonb, simpleMap.toJSON());
		} else if (typedValue.getN() != null) {
			String value = typedValue.getN();
			return new TableColumnValue(TableColumnType.numeric, value);
		} else if (typedValue.getNS() != null) {
			List<String> value = typedValue.getNS();
			return new TableColumnValue(TableColumnType.jsonb, Jackson.toJsonString(value));
		} else if (typedValue.getS() != null) {
			String value = typedValue.getS();
			return new TableColumnValue(TableColumnType.text, value);
		} else if (typedValue.getSS() != null) {
			List<String> value = typedValue.getSS();
			return new TableColumnValue(TableColumnType.jsonb, Jackson.toJsonString(value));
		} else {
			return null;
		}
	}

	public static TableColumnType columnTypeFromDynamoValue(AttributeValue typedValue) {
		if(typedValue.getB() != null) {
			return TableColumnType.bytea;
		} else if (typedValue.getBOOL() != null) {
			return TableColumnType.bool;
		} else if (typedValue.getBS() != null) {
			return TableColumnType.jsonb;
		} else if (typedValue.getL() != null) {
			return TableColumnType.jsonb;
		} else if (typedValue.getM() != null) {
			return TableColumnType.jsonb;
		} else if (typedValue.getN() != null) {
			return TableColumnType.numeric;
		} else if (typedValue.getNS() != null) {
			return TableColumnType.jsonb;
		} else if (typedValue.getS() != null) {
			return TableColumnType.text;
		} else if (typedValue.getSS() != null) {
			return TableColumnType.jsonb;
		} else {
			return TableColumnType.text;
		}
	}



}
