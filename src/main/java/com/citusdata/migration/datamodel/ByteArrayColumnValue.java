/**
 * 
 */
package com.citusdata.migration.datamodel;

/**
 * @author marco
 *
 */
public class ByteArrayColumnValue extends TableColumnValue {

	public ByteArrayColumnValue(byte[] value) {
		super(TableColumnType.bytea, value);
	}

	@Override
	public String toQuotedString() {
		return "'" + toString() + "'";
	}

	@Override
	public String toString() {
		return "\\x" + byteArrayToHex((byte[]) datum);
	}

	private final static char[] hexArray = "0123456789ABCDEF".toCharArray();
	public static String byteArrayToHex(byte[] bytes) {
		char[] hexChars = new char[bytes.length * 2];

		for ( int j = 0; j < bytes.length; j++ ) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = hexArray[v >>> 4];
			hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		}

		return new String(hexChars);
	}

}
