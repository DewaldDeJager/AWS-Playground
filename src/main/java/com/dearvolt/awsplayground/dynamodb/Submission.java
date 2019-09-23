package com.dearvolt.awsplayground.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.time.LocalDateTime;
import java.util.Map;

public class Submission {
    private String digitalID;
    private LocalDateTime timestamp;
    private String submission;
    private int version;
    private String channel;

    public Submission(String digitalID, LocalDateTime timestamp, String submission, int version, String channel) {
        this.digitalID = digitalID;
        this.timestamp = timestamp;
        this.submission = submission;
        this.version = version;
        this.channel = channel;
    }

    public Map<String, AttributeValue> toItem() {
        // TODO: Implement me :)
        throw new NotImplementedException();
    }
}
