/**
 * 
 */
package com.citusdata.migration.datamodel;

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author marco
 *
 */
public class TableRowBatch {

	final List<TableRow> tableRows;
	
	public TableRowBatch() {
		this.tableRows = new ArrayList<>();
	}
	
	public void addRow(TableRow tableRow) {
		this.tableRows.add(tableRow);
	}
	
	public String toCopyInput() {
		StringBuilder sb = new StringBuilder();
		
		for(TableRow tableRow : tableRows) {
			sb.append(tableRow.toCopyRow());
			sb.append('\n');
		}
		
		return sb.toString();
	}
	
	public Reader asCopyReader() {
		return new StringReader(toCopyInput());
	}

}
