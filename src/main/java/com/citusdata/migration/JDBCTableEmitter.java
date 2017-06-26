/**
 * 
 */
package com.citusdata.migration;

import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import com.citusdata.migration.datamodel.PrimaryKeyValue;
import com.citusdata.migration.datamodel.TableColumn;
import com.citusdata.migration.datamodel.TableEmitter;
import com.citusdata.migration.datamodel.TableRow;
import com.citusdata.migration.datamodel.TableSchema;

/**
 * @author marco
 *
 */
public class JDBCTableEmitter implements TableEmitter {

	final String url;

	Connection currentConnection;

	public JDBCTableEmitter(String url) throws SQLException {
		this.url = url;
		this.currentConnection = DriverManager.getConnection(url);
	}
	
	public void createTable(TableSchema tableSchema) throws SQLException {
		Statement statement = currentConnection.createStatement();

		for (String ddlCommand : tableSchema.toDDLList()) {
			statement.execute(ddlCommand);
		}
	}

	public long copyFromReader(TableSchema tableSchema, Reader reader) throws SQLException, IOException {
		String query = tableSchema.copyFromStdin();
		CopyManager copyManager = new CopyManager((BaseConnection) currentConnection);
		long numRows = copyManager.copyIn(query, reader);

		return numRows;
	}

	public void createColumn(TableColumn column) throws SQLException {
		String query = column.toAlterTableAddColumn();
		Statement statement = currentConnection.createStatement();
		statement.execute(query);
	}

	public void upsert(TableRow tableRow) throws SQLException {
		Statement statement = currentConnection.createStatement();
		statement.execute(tableRow.toUpsert());
	}

	public void delete(PrimaryKeyValue primaryKeyValue) throws SQLException {
		Statement statement = currentConnection.createStatement();
		statement.execute(primaryKeyValue.toDelete());
	}

}
