/**
 * 
 */
package com.citusdata.migration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;

import com.citusdata.migration.datamodel.PrimaryKeyValue;
import com.citusdata.migration.datamodel.TableColumn;
import com.citusdata.migration.datamodel.TableEmitter;
import com.citusdata.migration.datamodel.TableRow;
import com.citusdata.migration.datamodel.TableSchema;

/**
 * @author marco
 *
 */
public class StdoutSQLEmitter implements TableEmitter {

	public StdoutSQLEmitter() {
	}
	
	@Override
	public TableSchema fetchSchema(String tableName) {
		return null;
	}

	@Override
	public void createTable(TableSchema tableSchema) {
		System.out.println(tableSchema.toDDL());
	}

	@Override
	public void createColumn(TableColumn column) {
		System.out.println(column.toAlterTableAddColumn()+";");
	}

	@Override
	public long copyFromReader(TableSchema tableSchema, Reader reader) {
		long numLines = 0;
		String line = null;

		try (BufferedReader br = new BufferedReader(reader)) {
			System.out.println(tableSchema.copyFromStdin()+";");

			while ((line = br.readLine()) != null) {
				System.out.println(line);
				numLines++;
			}

			System.out.println("\\.\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return numLines;
	}

	@Override
	public void upsert(TableRow tableRow) {
		System.out.println(tableRow.toUpsert()+";");
	}

	@Override
	public void delete(PrimaryKeyValue primaryKeyValue) {
		System.out.println(primaryKeyValue.toDelete()+";");
	}

}
