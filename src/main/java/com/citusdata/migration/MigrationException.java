/**
 * 
 */
package com.citusdata.migration;

public abstract class MigrationException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7432723543474028751L;

	public MigrationException() {
	}

	public MigrationException(String message, Object... args) {
		super(String.format(message, args));
	}


}
