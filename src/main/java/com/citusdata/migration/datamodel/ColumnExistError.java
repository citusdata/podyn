/**
 * 
 */
package com.citusdata.migration.datamodel;

import com.citusdata.migration.MigrationError;

/**
 * @author marco
 *
 */
public class ColumnExistError extends MigrationError {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4618893346627250608L;

	public ColumnExistError() {
		super();
	}

	public ColumnExistError(String message, Object... args) {
		super(message, args);
	}

}
