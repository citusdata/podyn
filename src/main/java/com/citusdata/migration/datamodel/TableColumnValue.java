/**
 * 
 */
package com.citusdata.migration.datamodel;

/**
 * @author marco
 *
 */
public class TableColumnValue {

	public final TableColumnType type;
	public final Object datum;
	
	public TableColumnValue(TableColumnType type, Object value) {
		this.type = type;
		this.datum = value;
	}

	public String toCopyValue() {
		String string = toString();
		StringBuilder sb = new StringBuilder();
		
		for(int i = 0; i < string.length(); i++) {
			char ch = string.charAt(i);
			
			switch(ch) {
			case '\b':
				sb.append("\\b");
				break;
			case '\f':
				sb.append("\\f");
				break;
			case '\n':
				sb.append("\\n");
				break;
			case '\r':
				sb.append("\\r");
				break;
			case '\t':
				sb.append("\\t");
				break;
			case '\\':
				sb.append("\\\\");
				break;
			default:
				sb.append(ch);
				break;
			}
		}

		return sb.toString();
	}

	public String toQuotedString() {
		if(type == TableColumnType.bytea) {
			return "'" + toString() + "'";
		}
		
		StringBuilder sb = new StringBuilder();
		String string = toString();
		boolean escapePrefix = false;
		
		sb.append('\'');
		
		for(int i = 0; i < string.length(); i++) {
			char ch = string.charAt(i);
			
			switch(ch) {
			case '\b':
				sb.append("\\b");
				escapePrefix = true;
				break;
			case '\f':
				sb.append("\\f");
				escapePrefix = true;
				break;
			case '\n':
				sb.append("\\n");
				escapePrefix = true;
				break;
			case '\r':
				sb.append("\\r");
				escapePrefix = true;
				break;
			case '\t':
				sb.append("\\t");
				escapePrefix = true;
				break;
			case '\'':
				sb.append("''");
				break;
			case '\\':
				sb.append("\\\\");
				escapePrefix = true;
				break;
			default:
				sb.append(ch);
				break;
			}
		}

		sb.append('\'');
		
		String escapedString = sb.toString();
		
		if (escapePrefix) {
			escapedString = 'E' + escapedString;
		}
		
		return escapedString;
	}

	public String toString() {
		switch(type) {
		case bytea:
			return "\\x" + byteArrayToHex((byte[]) datum);

		case bool:
		case text:
		case jsonb:
		case numeric:
		default:
			return datum.toString();
		}
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
