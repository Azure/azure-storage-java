package com.microsoft.azure.storage.file;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    public String toJSON() {
        StringBuilder jsonPermission = new StringBuilder();
        jsonPermission.append("{\"permission\": \"").append(permission).append("\"}");
        return jsonPermission.toString();
    }

    public void fromJSON(String jsonPermission) {
        // TODO : Figure out better way to do this
        // Find permission json component
        Pattern p = Pattern.compile("(?:\"permission\":\")(.*?)(?:\")");
        Matcher m = p.matcher(jsonPermission);
        if (m.find()) {
            // Extract value from key-value pair
            String[] extractedData = m.group().split("\"permission\":");
            // Shave off quotation marks from json formatted String
            this.permission = extractedData[1].substring(1, extractedData[1].length()-1);
        }
    }
}
