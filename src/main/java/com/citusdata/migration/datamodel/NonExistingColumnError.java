/**
 * 
 */
package com.citusdata.migration.datamodel;

import com.citusdata.migration.MigrationError;

/**
 * @author marco
 *
 */
public class NonExistingColumnError extends MigrationError {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4618893346627250608L;

	public NonExistingColumnError() {
		super();
	}

	public NonExistingColumnError(String message, Object... args) {
		super(message, args);
	}

}
