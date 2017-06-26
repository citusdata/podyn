/**
 * 
 */
package com.citusdata.migration;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.internal.InternalUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.util.json.Jackson;
import com.citusdata.migration.datamodel.TableColumn;
import com.citusdata.migration.datamodel.TableColumnType;
import com.citusdata.migration.datamodel.TableColumnValue;
import com.citusdata.migration.datamodel.TableRow;
import com.citusdata.migration.datamodel.TableRowBatch;
import com.citusdata.migration.datamodel.TableSchema;

/**
 * @author marco
 *
 */
public class DynamoDBScanner {

	final String tableName;
	final AmazonDynamoDB dynamoDBClient;

	private Map<String, AttributeValue> lastEvaluatedKey;

	public DynamoDBScanner(AmazonDynamoDB dynamoDBClient, String tableName) {
		this.dynamoDBClient = dynamoDBClient;
		this.tableName = tableName;
	}

	private ScanResult scanWithRetries() {
		ScanRequest scanRequest = new ScanRequest().
				withTableName(tableName).
				withConsistentRead(true).
				withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL).
				withLimit(100).
				withExclusiveStartKey(this.lastEvaluatedKey);
		
		for (int tryNumber = 1; ; tryNumber++) {
			try {
				return dynamoDBClient.scan(scanRequest);
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

	public ScannedBatch scan() {
		final ScanResult scanResult = scanWithRetries();

		this.lastEvaluatedKey = scanResult.getLastEvaluatedKey();

		return new ScannedBatch() {

			@Override
			public List<TableColumn> addNewColumnsToSchema(TableSchema tableSchema) {
				Map<String,TableColumn> newColumns = new LinkedHashMap<>();

				for(Map<String,AttributeValue> dynamoItem : scanResult.getItems()) {
					for(Map.Entry<String,AttributeValue> dynamoField : dynamoItem.entrySet()) {
						String columnName = dynamoField.getKey();
						TableColumn column = tableSchema.getColumn(columnName);
						TableColumnType valueType = columnTypeFromDynamoValue(dynamoField.getValue());
						
						if (column == null) {
							column = tableSchema.addColumn(columnName, valueType);
							newColumns.put(columnName, column);
						} else if (column.type != valueType) {
							columnName = columnName + "_" + valueType;
							column = tableSchema.getColumn(columnName);
							
							if (column == null) {
								column = tableSchema.addColumn(columnName, valueType);
								newColumns.put(columnName, column);
							}
						}
					}
				}

				return new ArrayList<>(newColumns.values());
			}

			@Override
			public TableRowBatch rows(TableSchema tableSchema) {
				TableRowBatch batch = new TableRowBatch();

				for(Map<String,AttributeValue> dynamoItem : scanResult.getItems()) {
					TableRow tableRow = rowFromDynamoRecord(tableSchema, dynamoItem);

					batch.addRow(tableRow);
				}

				return batch;
			}

			@Override
			public double getConsumedCapacity() {
				return scanResult.getConsumedCapacity().getCapacityUnits();
			}

			@Override
			public boolean hasNextBatch() {
				return lastEvaluatedKey != null;
			}

		};
	}

	public static TableRow rowFromDynamoRecord(TableSchema tableSchema, Map<String,AttributeValue> dynamoItem) {
		TableRow row = tableSchema.createRow();

		for(Map.Entry<String, AttributeValue> entry : dynamoItem.entrySet()) {
			String columnName = entry.getKey();
			TableColumn column = tableSchema.getColumn(columnName);
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
