/**
 * 
 */
package com.citusdata.migration;

import java.util.List;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.Shard;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

/**
 * @author marco
 *
 */
public class DynamoDBCDC {

	final String tableName;
	final AmazonDynamoDBStreams streamsClient;
	final String tableStreamArn;
	
	private List<Shard> shards;

	public DynamoDBCDC(AmazonDynamoDB dynamoDBClient, AmazonDynamoDBStreams streamsClient, String tableName) {
		this.streamsClient = streamsClient;
		this.tableName = tableName;
		
		DescribeTableResult describeTableResult = dynamoDBClient.describeTable(tableName);
		TableDescription tableDescription = describeTableResult.getTable();
		this.tableStreamArn = tableDescription.getLatestStreamArn();

		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().
				withStreamArn(tableStreamArn);
		DescribeStreamResult describeStreamResult = streamsClient.describeStream(describeStreamRequest);
		this.shards = describeStreamResult.getStreamDescription().getShards();
	}
	
	public boolean scan() {

		return false;
	}

}
