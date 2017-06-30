/**
 * 
 */
package com.citusdata.migration.datamodel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author marco
 *
 */
public class TableSchema {

	public String schemaName;
	public String tableName;

	private final Map<String, TableColumn> columns;
	private TableColumn distributionColumn;
	private List<String> primaryKey;
	private List<TableIndex> tableIndexes;

	public TableSchema(String tableName) {
		this(tableName, null);
	}

	public TableSchema(String tableName, String schemaName) {
		this.tableName = tableName;
		this.schemaName = schemaName;
		this.columns = new LinkedHashMap<>();
		this.primaryKey = null;
		this.tableIndexes = new ArrayList<>();
	}

	public TableRow createRow() {
		return new TableRow(this);
	}

	public boolean columnExists(String columnName) {
		return columns.containsKey(columnName);
	}

	public TableColumn addColumn(String columnName, TableColumnType type) {
		if (columnExists(columnName)) {
			throw new ColumnExistError("column \"%s\" already exists", columnName);
		}

		TableColumn column = new TableColumn(this, columnName, type);
		this.columns.put(columnName, column);

		return column;
	}

	public TableColumn getDistributionColumn() {
		return this.distributionColumn;
	}

	public void setDistributionColumn(String columnName) {
		if (!columnExists(columnName)) {
			throw new NonExistingColumnError("distribution key column name \"%s\" does not exist", columnName);
		}
		if (this.distributionColumn != null) {
			throw new ColumnExistError("schema already has a distribution column \"%s\"", this.distributionColumn.name);
		}

		this.distributionColumn = columns.get(columnName);

	}

	public void setPrimaryKey(List<String> columnNames) {
		for (String columnName : columnNames) {
			if (!columnExists(columnName)) {
				throw new NonExistingColumnError("primary key column name \"%s\" does not exist", columnName);
			}
		}

		this.primaryKey = columnNames;
	}

	public void addIndex(String indexName, List<String> indexColumns) {
		this.tableIndexes.add(new TableIndex(this.tableName, indexName, indexColumns));
	}

	public static boolean requiresQuotes(String identifier) {
		for (int i = 0; i < identifier.length(); i++) {
			char ch = identifier.charAt(i);
			if (!((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || (ch == '_'))) {
				return true;
			}
		}

		int searchIndex = Arrays.binarySearch(PostgresKeywords.KEYWORDS, identifier.toLowerCase());
		if (searchIndex >= 0) {
			return true;
		}

		return false;
	}

	public static String quoteIdentifier(String identifier) {
		if (requiresQuotes(identifier)) {
			return '"' + identifier.replace("\"", "\"\"") + '"';
		} else {
			return identifier;
		}
	}

	public String getQualifiedTableName() {
		StringBuilder sb = new StringBuilder();

		if (schemaName != null) {
			sb.append(quoteIdentifier(schemaName));
			sb.append('.');
		}

		sb.append(quoteIdentifier(tableName));

		return sb.toString();
	}

	public String copyFromStdin() {
		StringBuilder sb = new StringBuilder();

		sb.append("COPY ");
		sb.append(getQualifiedTableName());
		sb.append(" FROM STDIN");

		return sb.toString();
	}

	public String toString() {
		return toDDL();
	}

	public String toDDL() {
		StringBuilder sb = new StringBuilder();

		for (String command : toDDLList()) {
			sb.append(command);
			sb.append(";\n");
		}

		return sb.toString();
	}

	public List<String> toDDLList() {
		List<String> ddlCommands = new ArrayList<>();

		ddlCommands.add(createTableDDL());

		for (TableIndex tableIndex : tableIndexes) {
			ddlCommands.add(tableIndex.toDDL());
		}

		if (distributionColumn != null) {
			StringBuilder sb = new StringBuilder();

			sb.append("SELECT create_distributed_table('");
			sb.append(getQualifiedTableName());
			sb.append("', '");
			sb.append(distributionColumn.name);
			sb.append("')");

			ddlCommands.add(sb.toString());
		}

		return ddlCommands;
	}

	public String createTableDDL() {
		StringBuilder sb = new StringBuilder();

		sb.append("CREATE TABLE ");
		sb.append(getQualifiedTableName());
		sb.append(" (");

		boolean skipComma = true;

		for (TableColumn column : columns.values()) {
			if (!skipComma) {
				sb.append(",");
			}

			sb.append("\n  ");
			sb.append(column.toDDL());

			skipComma = false;
		}

		if (primaryKey != null) {
			sb.append(",\n  PRIMARY KEY(");

			skipComma = true;

			for (String primaryKeyColumn : primaryKey) {
				if (!skipComma) {
					sb.append(", ");
				}

				sb.append(quoteIdentifier(primaryKeyColumn));

				skipComma = false;
			}

			sb.append(")");
		}

		sb.append("\n)");

		return sb.toString();
	}

	public String toDelete() {
		return toDelete(null);
	}

	public String toDelete(Map<String,TableColumnValue> primaryKeyValues) {
		if (primaryKeyValues != null && primaryKeyValues.size() != this.primaryKey.size()) {
			throw new InvalidNumberOfColumnsError("provided %d values while primary key has %d columns",
					primaryKeyValues.size(), this.primaryKey.size());
		}

		StringBuilder sb = new StringBuilder();

		sb.append("DELETE FROM ");
		sb.append(getQualifiedTableName());
		sb.append(" WHERE ");

		boolean skipSeparator = true;

		for (String columnName : getPrimaryKeyColumnNames()) {
			TableColumn column = columns.get(columnName);

			if (!skipSeparator) {
				sb.append(" AND ");
			}

			sb.append(quoteIdentifier(columnName));
			sb.append(" = ");

			if (primaryKeyValues != null) {
				sb.append(primaryKeyValues.get(columnName).toQuotedString());
				sb.append("::");
				sb.append(column.type);
			} else {
				sb.append('?');
			}

			skipSeparator = false;
		}

		return sb.toString();
	}

	public String toUpsert() {
		StringBuilder sb = new StringBuilder();

		sb.append(toInsert());
		sb.append(" ON CONFLICT (");

		boolean skipSeparator = true;

		for (String columnName : getPrimaryKeyColumnNames()) {
			if (!skipSeparator) {
				sb.append(", ");
			}

			sb.append(TableSchema.quoteIdentifier(columnName));

			skipSeparator = false;
		}

		sb.append(") DO UPDATE SET ");

		skipSeparator = true;

		for (String columnName : getColumnNames()) {
			if (isInPrimaryKey(columnName)) {
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
		sb.append(getQualifiedTableName());
		sb.append(" (");

		boolean skipSeparator = true;

		for (String columnName : getColumnNames()) {
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

		for (TableColumn column : columns.values()) {
			if (!skipSeparator) {
				sb.append(", ");
			}

			sb.append("?");

			skipSeparator = false;
		}

		sb.append(")");

		return sb.toString();
	}

	public Iterable<String> getColumnNames() {
		return this.columns.keySet();
	}

	public Iterable<String> getPrimaryKeyColumnNames() {
		return this.primaryKey;
	}

	public boolean isInPrimaryKey(String columnName) {
		return this.primaryKey.contains(columnName);
	}

	public Iterable<TableColumn> getColumns() {
		return this.columns.values();
	}

	public TableColumn getColumn(String keyName) {
		return columns.get(keyName);
	}

	public int columnCount() {
		return columns.size();
	}

	public boolean isPrimaryKeyColumn(String key) {
		return this.primaryKey.contains(key);
	}

}
