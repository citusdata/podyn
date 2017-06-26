/**
 * 
 */
package com.citusdata.migration.datamodel;

import java.util.Map;

/**
 * @author marco
 *
 */
public class Delete implements TableModification {

	final PrimaryKeyValue keyValue;
	
	public Delete(PrimaryKeyValue keyValue) {
		this.keyValue = keyValue;
	}

	public PrimaryKeyValue getKeyValue() {
		return keyValue;
	}

}
