/**
 * 
 */
package com.citusdata.migration;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.citusdata.migration.datamodel.TableEmitter;

/**
 * @author marco
 *
 */
public class DynamoDBReplicator {

	public static TableEmitter createEmitter(String url, int outgoingConnectionCount) throws SQLException {
		List<TableEmitter> emitters = new ArrayList<>();

		for(int i = 0; i < outgoingConnectionCount; i++) {
			emitters.add(new JDBCTableEmitter(url));
		}

		return new HashedMultiEmitter(emitters);
	}

	public static void main(String[] args) throws SQLException, IOException {
		Options options = new Options();

		Option help = new Option("h", "help", false, "Show help");
		help.setRequired(false);
		options.addOption(help);

		Option regionOption = new Option("r", "region", true, "AWS region");
		regionOption.setRequired(false);
		options.addOption(regionOption);

		Option dynamoDBTableOption = new Option("t", "table", true, "DynamoDB table name of the source");
		dynamoDBTableOption.setRequired(true);
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

		Option maxScanRateOption = new Option("sr", "scan-rate", false, "Maximum reads/sec during scan (default 25)");
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
			int maxScanRate = Integer.parseInt(cmd.getOptionValue("scan-rate", "25"));
			List<String> tableNames = Arrays.asList(cmd.getOptionValue("table").split(","));
			String postgresURL = cmd.getOptionValue("postgres-jdbc-url");

			String region = cmd.getOptionValue("region", "us-east-1");

			AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

			AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().
					withCredentials(credentialsProvider).
					withRegion(region).
					build();

			AmazonDynamoDBStreams streamsClient = AmazonDynamoDBStreamsClientBuilder.standard().
					withCredentials(credentialsProvider).
					withRegion(region).
					build();

			TableEmitter emitter;

			if (postgresURL != null) {
				emitter = createEmitter(postgresURL, 10);
			} else {
				emitter = new StdoutSQLEmitter();
			}

			List<DynamoDBTableReplicator> replicators = new ArrayList<>();

			for(String tableName : tableNames) {
				DynamoDBTableReplicator replicator = new DynamoDBTableReplicator(
						dynamoDBClient, streamsClient, emitter, tableName);

				replicators.add(replicator);
			}

			replicateSchema = true;
			if (replicateSchema) {
				for(DynamoDBTableReplicator replicator : replicators) {
					replicator.replicateSchema();
				}
			}

			if (replicateData) {
				for(DynamoDBTableReplicator replicator : replicators) {
					replicator.replicateData(maxScanRate);
				}
			}

			if (replicateChanges) {
				for(DynamoDBTableReplicator replicator : replicators) {
					replicator.replicateChanges();
				}
			}
		} catch (ParseException e) {
			System.out.println(e.getMessage());
			formatter.printHelp("dynamodb-to-postgres", options);
			System.exit(1);
		}
	}

}
