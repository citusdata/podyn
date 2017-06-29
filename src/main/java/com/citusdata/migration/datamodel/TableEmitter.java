package com.citusdata.migration.datamodel;

import java.io.Reader;

import com.citusdata.migration.EmissionException;

public interface TableEmitter {

	TableSchema fetchSchema(String tableName) throws EmissionException;
	void createTable(TableSchema tableSchema) throws EmissionException;
	void createColumn(TableColumn column) throws EmissionException;
	long copyFromReader(TableSchema tableSchema, Reader reader) throws EmissionException;
	void upsert(TableRow tableRow) throws EmissionException;
	void delete(PrimaryKeyValue primaryKeyValue) throws EmissionException;
}
