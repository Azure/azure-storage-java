/*
 * Copyright Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.azure.storage.file;

/**
 * Represents a handle to an Azure Storage File.
 */
public class FileHandle {

    private String handleID;

    private String path;

    private String fileID;

    private String parentFileID;

    private String sessionID;

    private String clientIP;

    private String openTime;

    private String lastReconnectTime;

    public String getHandleID() {
        return handleID;
    }

    void setHandleID(String handleID) {
        this.handleID = handleID;
    }

    public String getPath() {
        return path;
    }

    void setPath(String path) {
        this.path = path;
    }

    public String getFileID() {
        return fileID;
    }

    void setFileID(String fileID) {
        this.fileID = fileID;
    }

    public String getParentFileID() {
        return parentFileID;
    }

    void setParentFileID(String parentFileID) {
        this.parentFileID = parentFileID;
    }

    public String getSessionID() {
        return sessionID;
    }

    void setSessionID(String sessionID) {
        this.sessionID = sessionID;
    }

    public String getClientIP() {
        return clientIP;
    }

    void setClientIP(String clientIP) {
        this.clientIP = clientIP;
    }

    public String getOpenTime() {
        return openTime;
    }

    void setOpenTime(String openTime) {
        this.openTime = openTime;
    }

    public String getLastReconnectTime() {
        return lastReconnectTime;
    }

    void setLastReconnectTime(String lastReconnectTime) {
        this.lastReconnectTime = lastReconnectTime;
    }
}
