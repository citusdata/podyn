/**
 * 
 */
package com.citusdata.migration.datamodel;

import com.citusdata.migration.MigrationException;

/**
 * @author marco
 *
 */
public class TableExistsException extends MigrationException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4618893346627250608L;

	public TableExistsException(Exception e) {
		super(e);
	}

	public TableExistsException(String message, Object... args) {
		super(message, args);
	}

}
