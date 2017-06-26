/**
 * 
 */
package com.citusdata.migration.datamodel;

/**
 * @author marco
 *
 */
public class Upsert implements TableModification {

	final TableRow newValue;
	
	public Upsert(TableRow newValue) {
		this.newValue = newValue;
	}

	public TableRow getNewRow() {
		return newValue;
	}

}
