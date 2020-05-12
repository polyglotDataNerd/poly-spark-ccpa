package com.poly.utils;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import java.io.Serializable;

@DynamoDBTable(tableName = "sg-id-systemIdMapping-production")
public class SGIDService implements Serializable {

    private String sourceSystemId;
    private String uuid;

    public SGIDService() {
    }

    @DynamoDBHashKey(attributeName = "sourceSystemId")
    public String get_sysid() {
        return sourceSystemId;
    }

    public void set_sysid(String sourceSystemId) {
        this.sourceSystemId = sourceSystemId;
    }

    @DynamoDBAttribute(attributeName = "uuid")
    public String get_uuid() {
        return uuid;
    }

    public void set_uuid(String uuid) {
        this.uuid = uuid;
    }


}
