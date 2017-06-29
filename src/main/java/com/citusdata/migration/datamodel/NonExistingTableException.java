/**
 * 
 */
package com.citusdata.migration.datamodel;

import com.citusdata.migration.MigrationException;

/**
 * @author marco
 *
 */
public class NonExistingTableException extends MigrationException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4618893346627250608L;

	public NonExistingTableException(Exception e) {
		super(e);
	}

	public NonExistingTableException(String message, Object... args) {
		super(message, args);
	}

}
