/**
 * 
 */
package com.citusdata.migration;

import java.util.List;

import com.citusdata.migration.datamodel.TableColumn;
import com.citusdata.migration.datamodel.TableModification;
import com.citusdata.migration.datamodel.TableSchema;

/**
 * @author marco
 *
 */
public interface StreamedBatch {

	List<TableColumn> addNewColumns(TableSchema tableSchema);
	List<TableModification> modifications(TableSchema tableSchema);
	boolean hasNextBatch();
}
