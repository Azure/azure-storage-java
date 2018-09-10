/**
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
package com.microsoft.azure.storage;

import java.util.Date;
import java.util.UUID;

public class UserDelegationKey {

    /**
     * Object ID of this token.
     */
    private UUID signedOid;

    /**
     * Tenant UD if the tenant that issued this token.
     */
    private UUID signedTid;

    /**
     * The datetime this token becomes valid.
     */
    private Date signedStart;

    /**
     * The datetime this token expires.
     */
    private Date signedExpiry;

    /**
     * What service this key is valid for.
     */
    private String signedService;

    /**
     * The version identifier of the REST service that created this token.
     */
    private String signedVersion;

    /**
     * The user delegation key
     */
    private String value;


    public UUID getSignedOid() {
        return signedOid;
    }

    public void setSignedOid(UUID signedOid) {
        this.signedOid = signedOid;
    }

    public UUID getSignedTid() {
        return signedTid;
    }

    public void setSignedTid(UUID signedTid) {
        this.signedTid = signedTid;
    }

    public Date getSignedStart() {
        return signedStart;
    }

    public void setSignedStart(Date signedStart) {
        this.signedStart = signedStart;
    }

    public Date getSignedExpiry() {
        return signedExpiry;
    }

    public void setSignedExpiry(Date signedExpiry) {
        this.signedExpiry = signedExpiry;
    }

    public String getSignedService() {
        return signedService;
    }

    public void setSignedService(String signedService) {
        this.signedService = signedService;
    }

    public String getSignedVersion() {
        return signedVersion;
    }

    public void setSignedVersion(String signedVersion) {
        this.signedVersion = signedVersion;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
