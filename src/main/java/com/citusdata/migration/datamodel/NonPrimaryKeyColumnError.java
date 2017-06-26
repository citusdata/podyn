/**
 * 
 */
package com.citusdata.migration.datamodel;

import com.citusdata.migration.MigrationError;

/**
 * @author marco
 *
 */
public class NonPrimaryKeyColumnError extends MigrationError {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4618893346627250608L;

	public NonPrimaryKeyColumnError() {
		super();
	}

	public NonPrimaryKeyColumnError(String message, Object... args) {
		super(message, args);
	}

}
