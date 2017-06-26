package com.citusdata.migration;

import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.citusdata.migration.datamodel.PrimaryKeyValue;
import com.citusdata.migration.datamodel.TableColumn;
import com.citusdata.migration.datamodel.TableEmitter;
import com.citusdata.migration.datamodel.TableRow;
import com.citusdata.migration.datamodel.TableSchema;

/*
 * HashedMultiEmitter can be used to perform concurrent writes across a pool of
 * emitters. Rows with the same distribution key will always use the same
 * connection from the pool, such that write to a particular key are executed
 * in the order in which they are emitted, but writes across different keys may
 * be emitted concurrently.
 * 
 * When making schema changes or bulk loading, writes to other emitters are
 * blocked.
 */
public class HashedMultiEmitter implements TableEmitter {

	final List<TableEmitter> emitters;
	final ReadWriteLock lock;

	public HashedMultiEmitter(List<TableEmitter> emitters) {
		this.emitters = emitters;
		this.lock = new ReentrantReadWriteLock();

		if (emitters.isEmpty()) {
			throw new Error("emitters cannot be empty");
		}
	}

	@Override
	public void createTable(TableSchema tableSchema) throws SQLException {
		lock.writeLock().lock();

		try {
			TableEmitter emitter = emitters.get(0);
			emitter.createTable(tableSchema);
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public void createColumn(TableColumn column) throws SQLException {
		lock.writeLock().lock();

		try {
			TableEmitter emitter = emitters.get(0);
			
			emitter.createColumn(column);
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public long copyFromReader(TableSchema tableSchema, Reader reader) throws SQLException, IOException {
		lock.writeLock().lock();

		try {
			TableEmitter emitter = emitters.get(0);

			return emitter.copyFromReader(tableSchema, reader);
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public void upsert(TableRow tableRow) throws SQLException {
		lock.readLock().lock();

		try {
			String distributionColumnName = tableRow.tableSchema.getDistributionColumn().name;
			String distributionColumnValue = tableRow.getValue(distributionColumnName).toString();
			int emitterIndex = Math.abs(distributionColumnValue.hashCode()) % emitters.size();
			TableEmitter emitter = emitters.get(emitterIndex);

			synchronized (emitter) {
				emitter.upsert(tableRow);
			}
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public void delete(PrimaryKeyValue primaryKeyValue) throws SQLException {
		lock.readLock().lock();

		try {
			String distributionColumnName = primaryKeyValue.tableSchema.getDistributionColumn().name;
			String distributionColumnValue = primaryKeyValue.getValue(distributionColumnName).toString();
			int emitterIndex = Math.abs(distributionColumnValue.hashCode()) % emitters.size();
			TableEmitter emitter = emitters.get(emitterIndex);

			synchronized (emitter) {
				emitter.delete(primaryKeyValue);
			}
		} finally {
			lock.readLock().unlock();
		}
	}

}
