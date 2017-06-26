/**
 * 
 */
package com.citusdata.migration.datamodel;

import java.util.HashMap;
import java.util.Map;

/**
 * @author marco
 *
 */
public class PrimaryKeyValue {

	public final TableSchema tableSchema;

	final Map<String, TableColumnValue> values;

	public PrimaryKeyValue(TableSchema tableSchema) {
		this.tableSchema = tableSchema;
		this.values = new HashMap<String,TableColumnValue>();
	}

	public void setValue(String key, Object value) {
		TableColumn column = tableSchema.getColumn(key);
		
		if (!tableSchema.isPrimaryKeyColumn(key)) {
			throw new NonPrimaryKeyColumnError("column \"%s\" is not part of the primary key", key);
		}
		
		setValue(key, new TableColumnValue(column.type, value));
	}

	void setValue(String key, TableColumnValue value) {
		values.put(key, value);
	}
	
	public TableColumnValue getValue(String key) {
		return values.get(key);
	}

	public String toDelete() {
		StringBuilder sb = new StringBuilder();

		sb.append("DELETE FROM ");
		sb.append(tableSchema.getQualifiedTableName());
		sb.append(" WHERE ");

		boolean skipSeparator = true;

		for (String columnName : tableSchema.getPrimaryKeyColumnNames()) {
			TableColumn column = tableSchema.getColumn(columnName);

			if (!skipSeparator) {
				sb.append(" AND ");
			}

			sb.append(TableSchema.quoteIdentifier(columnName));
			sb.append(" = ");
			sb.append(getValue(columnName).toQuotedString());
			sb.append("::");
			sb.append(column.type);

			skipSeparator = false;
		}

		return sb.toString();
	}
}
