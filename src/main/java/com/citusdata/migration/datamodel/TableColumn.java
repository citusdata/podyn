/**
 * 
 */
package com.citusdata.migration.datamodel;

public class TableColumn {
	
	final TableSchema tableSchema;
	
	public final String name;
	public final TableColumnType type;
	public boolean notNull;

	public TableColumn(TableSchema tableSchema, String name, TableColumnType type) {
		this.tableSchema = tableSchema;
		this.name = name;
		this.type = type;
		this.notNull = false;
	}
	
	public Object getQuotedIdentifier() {
		return TableSchema.quoteIdentifier(name);
	}
	
	public String toAlterTableAddColumn() {
		StringBuilder sb = new StringBuilder();
		
		sb.append("ALTER TABLE ");
		sb.append(tableSchema.getQualifiedTableName());
		sb.append(" ADD COLUMN ");
		sb.append(toDDL());
		
		return sb.toString();
	}
	
	public String toDDL() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(TableSchema.quoteIdentifier(name));
		sb.append(" ");
		sb.append(type);
		
		if (notNull) {
			sb.append(" NOT NULL");
		}
		
		return sb.toString();
	}
	
	public String toString() {
		return toDDL();
	}

	
}
