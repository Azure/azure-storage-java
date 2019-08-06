package com.microsoft.azure.storage;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * RESERVED FOR INTERNAL USE. Represents a subsection of a batch response.
 */
public class BatchSubResponse {


    private int statusCode = -1;
    private String status = "";
    private Map<String, String> headers = new HashMap<>();
    private InputStream body;

    public int getStatusCode() {
        return statusCode;
    }

    void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public String getStatusMessage() {
        return status;
    }

    void setStatusMessage(String status) {
        this.status = status;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    public InputStream getBody() {
        return body;
    }

    void setBody(InputStream body) {
        this.body = body;
    }
}
