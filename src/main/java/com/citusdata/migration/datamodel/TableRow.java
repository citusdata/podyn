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
public class TableRow {

	public final TableSchema tableSchema;

	private final Map<String, TableColumnValue> values;

	TableRow(TableSchema tableSchema) {
		this.tableSchema = tableSchema;
		this.values = new HashMap<>();
	}

	public void setValue(String key, Object value) {
		TableColumn column = tableSchema.getColumn(key);
		setValue(key, new TableColumnValue(column.type, value));
	}

	public void setValue(String key, TableColumnValue value) {
		values.put(key, value);
	}
	
	public String toUpsert() {
		StringBuilder sb = new StringBuilder();

		sb.append(toInsert());
		sb.append(" ON CONFLICT (");

		boolean skipSeparator = true;

		for (String columnName : tableSchema.getPrimaryKeyColumnNames()) {
			if (!skipSeparator) {
				sb.append(", ");
			}

			sb.append(TableSchema.quoteIdentifier(columnName));

			skipSeparator = false;
		}

		sb.append(") DO UPDATE SET ");

		skipSeparator = true;

		for (String columnName : tableSchema.getColumnNames()) {
			if (tableSchema.isInPrimaryKey(columnName)) {
				continue;
			}

			if (!skipSeparator) {
				sb.append(", ");
			}

			sb.append(TableSchema.quoteIdentifier(columnName));
			sb.append(" = EXCLUDED.");
			sb.append(TableSchema.quoteIdentifier(columnName));

			skipSeparator = false;
		}

		return sb.toString();
	}

	public String toInsert() {
		StringBuilder sb = new StringBuilder();

		sb.append("INSERT INTO ");
		sb.append(tableSchema.getQualifiedTableName());
		sb.append(" (");

		boolean skipSeparator = true;

		for (String columnName : tableSchema.getColumnNames()) {
			if (!skipSeparator) {
				sb.append(", ");
			}

			sb.append(TableSchema.quoteIdentifier(columnName));

			skipSeparator = false;
		}

		sb.append(") VALUES ");
		sb.append(toValues());

		return sb.toString();
	}

	public String toValues() {
		StringBuilder sb = new StringBuilder();

		sb.append("(");

		boolean skipSeparator = true;

		for (TableColumn column : tableSchema.getColumns()) {
			if (!skipSeparator) {
				sb.append(", ");
			}

			TableColumnValue value = values.get(column.name);

			if (value != null) {
				sb.append(value.toQuotedString());
				sb.append("::");
				sb.append(column.type);
			} else {
				sb.append("NULL");
			}

			skipSeparator = false;
		}

		sb.append(")");

		return sb.toString();
	}

	public String toCopyRow() {
		StringBuilder sb = new StringBuilder();

		boolean skipSeparator = true;

		for (String columnName : tableSchema.getColumnNames()) {
			if (!skipSeparator) {
				sb.append('\t');
			}

			TableColumnValue value = values.get(columnName);

			if (value != null) {
				String escapedString = value.toCopyValue();
				sb.append(escapedString);
			} else {
				sb.append("\\N");
			}
			
			skipSeparator = false;
		}

		return sb.toString();
	}

	public TableColumnValue getValue(String name) {
		return values.get(name);
	}

	public boolean hasValue(String columnName) {
		return values.containsKey(columnName);
	}

}
