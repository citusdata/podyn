/**
 * 
 */
package com.citusdata.migration.datamodel;

import com.citusdata.migration.MigrationError;

/**
 * @author marco
 *
 */
public class InvalidNumberOfColumnsError extends MigrationError {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4618893346627250608L;

	public InvalidNumberOfColumnsError() {
		super();
	}

	public InvalidNumberOfColumnsError(String message, Object... args) {
		super(message, args);
	}

}
