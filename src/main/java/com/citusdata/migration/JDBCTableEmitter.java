/**
 * 
 */
package com.citusdata.migration;

import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import com.citusdata.migration.datamodel.PrimaryKeyValue;
import com.citusdata.migration.datamodel.TableColumn;
import com.citusdata.migration.datamodel.TableColumnType;
import com.citusdata.migration.datamodel.TableEmitter;
import com.citusdata.migration.datamodel.TableRow;
import com.citusdata.migration.datamodel.TableSchema;

/**
 * @author marco
 *
 */
public class JDBCTableEmitter implements TableEmitter {

	final String DESCRIBE_TABLE_SQL = ""
			+ "SELECT "
			+ "  c.column_name, "
			+ "  c.data_type, "
			+ "  c.is_nullable = 'YES' AS is_nullable, "
			+ "  pk.column_name IS NOT NULL AS is_primary_key "
			+ "FROM "
			+ "  information_schema.columns c "
			+ "LEFT JOIN ("
			+ "  SELECT"
			+ "    ku.table_catalog,ku.table_schema, ku.table_name, ku.column_name "
			+ "  FROM"
			+ "    information_schema.table_constraints AS tc "
			+ "  INNER JOIN "
			+ "    information_schema.key_column_usage AS ku "
			+ "  ON "
			+ "    tc.constraint_type = 'PRIMARY KEY' AND "
			+ "    tc.constraint_name = ku.constraint_name"
			+ ") pk "
			+ "ON "
			+ "  c.table_schema = pk.table_schema AND "
			+ "  c.table_name = pk.table_name AND "
			+ "  c.column_name = pk.column_name "
			+ "WHERE"
			+ "  c.table_name = ? AND"
			+ "  c.table_schema = 'public' "
			+ "ORDER BY"
			+ "  ordinal_position";

	final String HAS_CITUS_SQL = ""
			+ "SELECT 1 FROM pg_extension WHERE extname = 'citus'";

	final String DISTRIBUTION_COLUMN_SQL = ""
			+ "SELECT "
			+ "  column_to_column_name(logicalrelid, partkey) "
			+ "FROM "
			+ "  pg_dist_partition "
			+ "WHERE "
			+ "  logicalrelid = ?::regclass";	

	final String url;

	final Connection currentConnection;
	final PreparedStatement describeTableStatement;
	final PreparedStatement hasCitusStatement;
	final PreparedStatement distributionColumnStatement;

	public JDBCTableEmitter(String url) throws SQLException {
		this.url = url;
		this.currentConnection = DriverManager.getConnection(url);
		this.describeTableStatement = currentConnection.prepareStatement(DESCRIBE_TABLE_SQL);
		this.hasCitusStatement = currentConnection.prepareStatement(HAS_CITUS_SQL);
		this.distributionColumnStatement = currentConnection.prepareStatement(DISTRIBUTION_COLUMN_SQL);
	}

	public synchronized TableSchema fetchSchema(String tableName) {
		try {
			describeTableStatement.setString(1, tableName);

			ResultSet describeTableResults = describeTableStatement.executeQuery();

			if (describeTableResults.next()) {
				TableSchema tableSchema = new TableSchema(tableName);
				List<String> primaryKeyColumns = new ArrayList<>();

				do {
					String columnName = describeTableResults.getString("column_name");
					String typeName = describeTableResults.getString("data_type");
					boolean isNullable = describeTableResults.getBoolean("is_nullable");
					boolean isPrimaryKey = describeTableResults.getBoolean("is_primary_key");

					TableColumnType columnType = TableColumnType.fromName(typeName);
					TableColumn column = tableSchema.addColumn(columnName, columnType);
					column.notNull = !isNullable;

					if (isPrimaryKey) {
						primaryKeyColumns.add(column.name);
					}

				} while(describeTableResults.next());

				tableSchema.setPrimaryKey(primaryKeyColumns);

				if (hasCitus()) {
					String distributionColumnName = getDistributionColumn(tableName);

					if (distributionColumnName != null) {
						tableSchema.setDistributionColumn(distributionColumnName);
					}
				}

				return tableSchema;
			} else {
				return null;
			}
		} catch (SQLException e) {
			throw new EmissionException(e);
		}
	}

	public synchronized boolean hasCitus() {
		try {
			ResultSet hasCitusResults = hasCitusStatement.executeQuery();
			return hasCitusResults.next();
		} catch (SQLException e) {
			throw new EmissionException(e);
		}
	}

	public synchronized String getDistributionColumn(String tableName) {
		try {
			String distributionColumnName = null;

			distributionColumnStatement.setString(1, tableName);

			ResultSet distributionColumnResult = distributionColumnStatement.executeQuery();

			if (distributionColumnResult.next()) {
				distributionColumnName = distributionColumnResult.getString(1);
			}

			return distributionColumnName;
		} catch (SQLException e) {
			throw new EmissionException(e);
		}
	}

	public synchronized void createTable(TableSchema tableSchema) throws EmissionException {
		try {
			Statement statement = currentConnection.createStatement();

			for (String ddlCommand : tableSchema.toDDLList()) {
				statement.execute(ddlCommand);
			}
		} catch (SQLException e) {
			throw new EmissionException(e);
		}
	}

	public synchronized long copyFromReader(TableSchema tableSchema, Reader reader) {
		try {
			String query = tableSchema.copyFromStdin();
			CopyManager copyManager = new CopyManager((BaseConnection) currentConnection);
			long numRows = copyManager.copyIn(query, reader);

			return numRows;
		} catch (Exception e) {
			throw new EmissionException(e);
		}
	}

	public synchronized void createColumn(TableColumn column) {
		try {
			String query = column.toAlterTableAddColumn();
			Statement statement = currentConnection.createStatement();
			statement.execute(query);
		} catch (SQLException e) {
			throw new EmissionException(e);
		}
	}

	public synchronized void upsert(TableRow tableRow) {
		try {
			Statement statement = currentConnection.createStatement();
			statement.execute(tableRow.toUpsert());
		} catch (SQLException e) {
			throw new EmissionException(e);
		}
	}

	public synchronized void delete(PrimaryKeyValue primaryKeyValue) {
		try {
			Statement statement = currentConnection.createStatement();
			statement.execute(primaryKeyValue.toDelete());
		} catch (SQLException e) {
			throw new EmissionException(e);
		}
	}
}
