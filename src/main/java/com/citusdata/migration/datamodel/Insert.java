/**
 * 
 */
package com.citusdata.migration.datamodel;

/**
 * @author marco
 *
 */
public class Insert implements TableModification {

	final TableRow newValue;
	
	public Insert(TableRow newValue) {
		this.newValue = newValue;
	}

}
