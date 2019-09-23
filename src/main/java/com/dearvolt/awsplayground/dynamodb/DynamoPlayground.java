package com.dearvolt.awsplayground.dynamodb;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.*;

enum AttributeNames {
    DIGITAL_ID("DigitalID"),
    TIMESTAMP("Timestamp");

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
        // TODO
    }

    // aws dynamodb describe-table --table-name TermsAndConditionsSubmissions --endpoint-url http://localhost:8000
    private void describeTable() {
        System.out.println("\nDESCRIBING TABLE\n");
        System.out.println(dbClient.describeTable(termsAndConditionsTableName));
        System.out.println("\nDONE DESCRIBING TABLE\n");
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
