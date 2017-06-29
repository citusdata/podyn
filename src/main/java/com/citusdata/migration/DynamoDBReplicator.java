/**
 * 
 */
package com.citusdata.migration;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.citusdata.migration.datamodel.NonExistingTableException;
import com.citusdata.migration.datamodel.TableEmitter;
import com.citusdata.migration.datamodel.TableExistsException;

/**
 * @author marco
 *
 */
public class DynamoDBReplicator {

	private static final Log LOG = LogFactory.getLog(DynamoDBReplicator.class);

	public static void main(String[] args) throws SQLException, IOException {
		Options options = new Options();

		Option help = new Option("h", "help", false, "Show help");
		help.setRequired(false);
		options.addOption(help);

		Option dynamoDBTableOption = new Option("t", "table", true, "DynamoDB table name(s) to replicate");
		dynamoDBTableOption.setRequired(false);
		options.addOption(dynamoDBTableOption);

		Option postgresURLOption = new Option("u", "postgres-jdbc-url", true, "PostgreSQL JDBC URL of the destination");
		postgresURLOption.setRequired(false);
		options.addOption(postgresURLOption);

		Option noSchemaOption = new Option("ns", "no-schema", false, "Skip initial schema creation");
		noSchemaOption.setRequired(false);
		options.addOption(noSchemaOption);

		Option noDataOption = new Option("nd", "no-data", false, "Skip initial data load");
		noDataOption.setRequired(false);
		options.addOption(noDataOption);

		Option noChangesOption = new Option("nc", "no-changes", false, "Skip streaming changes");
		noChangesOption.setRequired(false);
		options.addOption(noChangesOption);

		Option noAddColumnsOption = new Option("na", "no-add-columns", false, "Do not add columns to the initial schema");
		noAddColumnsOption.setRequired(false);
		options.addOption(noAddColumnsOption);

		Option citusOption = new Option("c", "citus", false, "Create distributed tables using Citus");
		citusOption.setRequired(false);
		options.addOption(citusOption);

		Option numConnectionsOption = new Option("n", "num-connections", true, "Number of connections to the database (default 16)");
		numConnectionsOption.setRequired(false);
		options.addOption(numConnectionsOption);

		Option maxScanRateOption = new Option("r", "scan-rate", true, "Maximum reads/sec during scan (default 25)");
		maxScanRateOption.setRequired(false);
		options.addOption(maxScanRateOption);

		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();

		try {
			CommandLine cmd = parser.parse(options, args);

			if (cmd.hasOption("help")) {
				formatter.printHelp("dynamodb-to-postgres", options);
				return;
			}

			boolean replicateSchema = !cmd.hasOption("no-schema");
			boolean replicateData = !cmd.hasOption("no-data");
			boolean replicateChanges = !cmd.hasOption("no-changes");
			boolean addColumnsEnabled = !cmd.hasOption("no-add-columns");
			boolean useCitus = cmd.hasOption("citus");
			int maxScanRate = Integer.parseInt(cmd.getOptionValue("scan-rate", "25"));
			int dbConnectionCount = Integer.parseInt(cmd.getOptionValue("num-connections", "16"));
			String tableNamesString = cmd.getOptionValue("table");
			String postgresURL = cmd.getOptionValue("postgres-jdbc-url");

			AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

			AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().
					withCredentials(credentialsProvider).
					build();

			AmazonDynamoDBStreams streamsClient = AmazonDynamoDBStreamsClientBuilder.standard().
					withCredentials(credentialsProvider).
					build();

			TableEmitter emitter;

			if (postgresURL != null) {
				List<TableEmitter> emitters = new ArrayList<>();

				for(int i = 0; i < dbConnectionCount; i++) {
					emitters.add(new JDBCTableEmitter(postgresURL));
				}

				emitter = new HashedMultiEmitter(emitters);
			} else {
				emitter = new StdoutSQLEmitter();
			}

			List<DynamoDBTableReplicator> replicators = new ArrayList<>();
			
			List<String> tableNames = new ArrayList<>();
			
			if (tableNamesString != null) {
				tableNames = Arrays.asList(tableNamesString.split(","));
			} else {
				ListTablesResult listTablesResult = dynamoDBClient.listTables();
				for(String tableName : listTablesResult.getTableNames()) {
					if (!tableName.startsWith(DynamoDBTableReplicator.LEASE_TABLE_PREFIX)) {
						tableNames.add(tableName);
					}
				}
			}
			
			ExecutorService executor = Executors.newCachedThreadPool();

			for(String tableName : tableNames) {
				DynamoDBTableReplicator replicator = new DynamoDBTableReplicator(
						dynamoDBClient, streamsClient, credentialsProvider, executor, emitter, tableName);

				replicator.setAddColumnEnabled(addColumnsEnabled);
				replicator.setUseCitus(useCitus);

				replicators.add(replicator);
			}

			if (replicateSchema) {
				for(DynamoDBTableReplicator replicator : replicators) {
					LOG.info(String.format("Constructing table schema for table %s", replicator.dynamoTableName));
					
					replicator.replicateSchema();

					LOG.info(replicator.tableSchema);
				}
			}

			if (replicateData) {
				for(DynamoDBTableReplicator replicator : replicators) {
					LOG.info(String.format("Moving data for table %s", replicator.dynamoTableName));
					replicator.replicateData(maxScanRate);
				}
			}

			if (replicateChanges) {
				for(DynamoDBTableReplicator replicator : replicators) {
					LOG.info(String.format("Replicating changes for table %s", replicator.dynamoTableName));
					replicator.replicateChanges();
				}
			}
		} catch (ParseException e) {
			LOG.error(e.getMessage());
			formatter.printHelp("dynamodb-to-postgres", options);
			System.exit(3);
		} catch (TableExistsException|NonExistingTableException e) {
			LOG.error(e.getMessage());
			System.exit(1);
		} catch (RuntimeException e) {
			e.printStackTrace();
			System.exit(2);
		} catch (Exception e) {
			LOG.error(e);
			System.exit(1);
		}
	}

}
