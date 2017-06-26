/**
 * 
 */
package com.citusdata.migration;

import java.util.List;
import java.util.Map;

import com.citusdata.migration.datamodel.TableColumn;
import com.citusdata.migration.datamodel.TableRowBatch;
import com.citusdata.migration.datamodel.TableSchema;

/**
 * @author marco
 *
 */
public interface ScannedBatch {

	List<TableColumn> addNewColumnsToSchema(TableSchema tableSchema);
	TableRowBatch rows(TableSchema tableSchema);
	double getConsumedCapacity();
	boolean hasNextBatch();
}
