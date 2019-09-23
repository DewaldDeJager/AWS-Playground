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
    private DynamoDB dynamoDB = new DynamoDB(AmazonDynamoDBClientBuilder.defaultClient());
    private AmazonDynamoDB dbClient = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "eu-west-1")
    ).build();
    private Random random = new Random(1337);

    public static void main(String[] args) {
        System.out.println("xD");
        DynamoPlayground playground = new DynamoPlayground();
        try {
            playground.createTestTable();
        } catch (ResourceInUseException e) {
            System.err.println("Table already exists. Moving on...\n");
        }
        playground.createTestItems();
        playground.describeTable();
        playground.getAllItems();
    }

    private boolean hasUserAcceptedTermsAndConditions(String digitalId, int version) {
        QuerySpec spec = new QuerySpec()
                .withHashKey("DigitalID", digitalId)
                .withQueryFilters(
                        new QueryFilter("Submission").eq("ACCEPT"),
                        new QueryFilter("Version").eq(version)
                );
        Table termsAndConditionsTable = dynamoDB.getTable(termsAndConditionsTableName);
        ItemCollection<QueryOutcome> items = termsAndConditionsTable.query(spec);

        return items.getAccumulatedItemCount() > 0;
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
