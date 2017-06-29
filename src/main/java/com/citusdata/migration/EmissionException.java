/**
 * 
 */
package com.citusdata.migration;

public class EmissionException extends MigrationException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7432723543474028751L;

	public EmissionException(Exception e) {
		super(e);
	}

	public EmissionException(String message, Object... args) {
		super(String.format(message, args));
	}


}
