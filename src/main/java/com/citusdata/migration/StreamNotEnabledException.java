/**
 * 
 */
package com.citusdata.migration;

import com.citusdata.migration.MigrationException;

/**
 * @author marco
 *
 */
public class StreamNotEnabledException extends MigrationException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4618893346627250608L;

	public StreamNotEnabledException(String message, Object... args) {
		super(message, args);
	}

}
