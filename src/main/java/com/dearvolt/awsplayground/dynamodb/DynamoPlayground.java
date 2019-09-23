package com.dearvolt.awsplayground.dynamodb;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.*;

import java.time.LocalDateTime;
import java.util.*;

enum AttributeNames {
    DIGITAL_ID("DigitalID"),
    TIMESTAMP("Timestamp"),
    SUBMISSION("Submission"),
    VERSION("Version", ScalarAttributeType.N),
    CHANNEL("Channel");

    private String name;
    private ScalarAttributeType type;

    AttributeNames(String name, ScalarAttributeType type) {
        this.name = name;
        this.type = type;
    }

    AttributeNames(String name) {
        this(name, ScalarAttributeType.S);
    }

    public String getAttributeName() {
        return name;
    }

    public ScalarAttributeType getAttributeType() {
        return type;
    }

    public AttributeDefinition toAttributeDefinition() {
        return new AttributeDefinition(getAttributeName(), getAttributeType());
    }
}

public class DynamoPlayground {
    private final String termsAndConditionsTableName = "TermsAndConditionsSubmissions";
    private AmazonDynamoDB dbClient = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "eu-west-1")
    ).build();
    private Random random = new Random(1337);

    public static void main(String[] args) {
        System.out.println("xD");
        DynamoPlayground playground = new DynamoPlayground();
        try {
            playground.createTestTable();
            playground.createTestItems();
        } catch (ResourceInUseException e) {
            System.err.println("Table already exists. Moving on...\n");
        }
        playground.describeTable();
        playground.getAllItems();

        playground.getSingleItem("14", "2019-09-23T14:07:53.325");

        System.out.println("\nTesting query...");
        System.out.println("TRUE == " + playground.hasUserAcceptedTermsAndConditions("1", 69));
        System.out.println("FALSE == " + playground.hasUserAcceptedTermsAndConditions("14", 69));
        System.out.println("FALSE == " + playground.hasUserAcceptedTermsAndConditions("8", 68));
    }

    private Map<String, AttributeValue> getSingleItem(String digitalId, String timestamp) {
        Map<String, AttributeValue> query = new HashMap<>();
        query.put(AttributeNames.DIGITAL_ID.getAttributeName(), new AttributeValue().withS(digitalId));
        query.put(AttributeNames.TIMESTAMP.getAttributeName(), new AttributeValue().withS(timestamp));
        GetItemRequest request = new GetItemRequest()
                .withTableName(termsAndConditionsTableName)
                .withKey(query);
        GetItemResult response = dbClient.getItem(request);
        return response.getItem();
    }

    private boolean hasUserAcceptedTermsAndConditions(String digitalId, int version) {
        // Set up an alias for the partition key name in case it's a reserved word
        HashMap<String, String> attrNameAlias = new HashMap<>();
        attrNameAlias.put("#digitalid", AttributeNames.DIGITAL_ID.getAttributeName());
        attrNameAlias.put("#submission", AttributeNames.SUBMISSION.getAttributeName());
        attrNameAlias.put("#version", AttributeNames.VERSION.getAttributeName());

        // Set up mapping of the partition name with the value
        HashMap<String, AttributeValue> attrValues = new HashMap<>();
        attrValues.put(":" + AttributeNames.DIGITAL_ID.getAttributeName(), new AttributeValue().withS(digitalId));
        attrValues.put(":" + AttributeNames.SUBMISSION.getAttributeName(), new AttributeValue().withS("ACCEPT"));
        attrValues.put(":" + AttributeNames.VERSION.getAttributeName(), new AttributeValue().withN(String.valueOf(version)));

        QueryRequest request = new QueryRequest()
                .withTableName(termsAndConditionsTableName)
                .withKeyConditionExpression("#digitalid = :DigitalID")
                .withFilterExpression("#submission = :Submission AND #version = :Version")
                .withExpressionAttributeNames(attrNameAlias)
                .withExpressionAttributeValues(attrValues);

        QueryResult result = dbClient.query(request);

        return result.getCount() > 0;
    }

    private void createTestTable() {
        System.out.println("\nCREATING TABLE\n");
        CreateTableRequest request = new CreateTableRequest()
                .withAttributeDefinitions(
                        AttributeNames.DIGITAL_ID.toAttributeDefinition(),
                        AttributeNames.TIMESTAMP.toAttributeDefinition())
                .withKeySchema(
                        new KeySchemaElement(AttributeNames.DIGITAL_ID.getAttributeName(), KeyType.HASH),
                        new KeySchemaElement(AttributeNames.TIMESTAMP.getAttributeName(), KeyType.RANGE))
                .withBillingMode("PAY_PER_REQUEST")
                .withTableName(termsAndConditionsTableName);
        dbClient.createTable(request);
        System.out.println("\nDONE CREATING TABLE\n");
    }

    private void createTestItems() {
        System.out.println("\nCREATING TEST ITEMS\n");
        List<WriteRequest> itemWriteRequests = new ArrayList<>();
        for (int i = 1; i < 26; i++) {
            itemWriteRequests.add(new WriteRequest(createRandomItemRequest(String.valueOf(i))));
        }

        Map<String, List<WriteRequest>> tableWriteRequest = new HashMap<>();
        tableWriteRequest.put(termsAndConditionsTableName, itemWriteRequests);

        BatchWriteItemRequest request = new BatchWriteItemRequest(tableWriteRequest);
        dbClient.batchWriteItem(request);
        System.out.println("\nDONE CREATING TEST ITEMS\n");
    }

    private PutRequest createRandomItemRequest(String digitalId) {
        // TODO: Add random data
        Map<String, AttributeValue> randomItem = new HashMap<>();
        randomItem.put(AttributeNames.DIGITAL_ID.getAttributeName(), new AttributeValue().withS(digitalId));
        randomItem.put(AttributeNames.TIMESTAMP.getAttributeName(), new AttributeValue().withS(LocalDateTime.now().toString()));
        randomItem.put(AttributeNames.SUBMISSION.getAttributeName(), new AttributeValue().withS(random.nextFloat() < 0.05 ? "REJECT" : "ACCEPT"));
        randomItem.put(AttributeNames.VERSION.getAttributeName(), new AttributeValue().withN("69"));
        randomItem.put(AttributeNames.CHANNEL.getAttributeName(), new AttributeValue().withS("DERP"));

        return new PutRequest().withItem(randomItem);
    }

    private void addItem(Submission submission) {
        System.out.println("\nADDING NEW ITEM\n");
        PutItemRequest request = new PutItemRequest()
                .withTableName(termsAndConditionsTableName)
                .withItem(submission.toItem());
        dbClient.putItem(request);
        System.out.println("\nDONE ADDING NEW ITEM\n");
    }

    // aws dynamodb describe-table --table-name TermsAndConditionsSubmissions --endpoint-url http://localhost:8000
    private void describeTable() {
        System.out.println("\nDESCRIBING TABLE\n");
        System.out.println(dbClient.describeTable(termsAndConditionsTableName));
        System.out.println("\nDONE DESCRIBING TABLE\n");
    }

    private void deleteTable() {
        dbClient.deleteTable(termsAndConditionsTableName);
    }

    // aws dynamodb scan --table-name TermsAndConditionsSubmissions --endpoint-url http://localhost:8000
    private void getAllItems() {
        System.out.println("\nSCANNING TABLE\n");
        ScanRequest request = new ScanRequest()
                .withTableName(termsAndConditionsTableName);
        System.out.println(dbClient.scan(request));
        System.out.println("\nDONE SCANNING TABLE\n");
    }
}
