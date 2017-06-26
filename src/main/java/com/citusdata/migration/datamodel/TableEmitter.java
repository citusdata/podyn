package com.citusdata.migration.datamodel;

import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;
import java.util.Map;

public interface TableEmitter {

	void createTable(TableSchema tableSchema) throws SQLException;
	void createColumn(TableColumn column) throws SQLException;
	long copyFromReader(TableSchema tableSchema, Reader reader) throws SQLException, IOException;
	void upsert(TableRow tableRow) throws SQLException;
	void delete(PrimaryKeyValue primaryKeyValue) throws SQLException;
}
