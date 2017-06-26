/**
 * 
 */
package com.citusdata.migration;

public abstract class MigrationError extends Error {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1067423033089333254L;

	public MigrationError() {
	}

	public MigrationError(String message, Object... args) {
		super(String.format(message, args));
	}


}
