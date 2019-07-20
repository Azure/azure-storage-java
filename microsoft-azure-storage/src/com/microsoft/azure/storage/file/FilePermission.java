package com.microsoft.azure.storage.file;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class FilePermission {
    private String permission;

    public FilePermission() {
        // empty object
    }

    public FilePermission(String permission) {
        this.permission = permission;
    }

    public String getPermission() {
        return permission;
    }

    public void setPermission(String permission) {
        this.permission = permission;
    }

    public String toJSON() throws IOException {
        JsonFactory factory = new JsonFactory();
        ByteArrayOutputStream jsonStream = new ByteArrayOutputStream();
        JsonGenerator jg = factory.createGenerator(jsonStream);
        jg.writeStartObject();
        jg.writeStringField("permission", this.permission);
        jg.writeEndObject();
        jg.close();
        return jsonStream.toString();
    }

    public void fromJSON(String jsonPermission) throws IOException {
        JsonFactory factory = new JsonFactory();
        JsonParser jp = factory.createParser(jsonPermission);
        if (jp.nextToken() != JsonToken.START_OBJECT) {
            throw new IOException("Expected data to start with an Object");
        }
        while (jp.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = jp.getCurrentName();
            if(fieldName.equals("permission")){
                this.permission = jp.getValueAsString();
            }
        }
        jp.close();
    }
}
