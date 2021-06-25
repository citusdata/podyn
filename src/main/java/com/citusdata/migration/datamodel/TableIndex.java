package com.citusdata.migration.datamodel;

import java.util.List;

public class TableIndex {

	public final String tableName;
	public final String name;

	private final List<String> columnNames;

	public TableIndex(String tableName, String name, List<String> columnNames) {
		this.tableName = tableName;
		this.name = String.join("_", this.tableName, name);
		this.columnNames = columnNames;
	}
	
	public String toDDL() {
		StringBuilder sb = new StringBuilder();

		sb.append("CREATE INDEX ");
		sb.append(TableSchema.quoteIdentifier(name));
		sb.append(" ON ");
		sb.append(TableSchema.quoteIdentifier(tableName));
		sb.append(" (");
		
		boolean skipComma = true;
		
		for(String columnName : columnNames) {
			if (!skipComma) {
				sb.append(", ");
			}
			
			sb.append(TableSchema.quoteIdentifier(columnName));
			
			skipComma = false;
		}
		
		sb.append(")");
		
		return sb.toString();
	}
	
	public String toString() { 
		return toDDL();
	}

}
