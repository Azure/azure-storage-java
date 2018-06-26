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

/**
 * Represents the service properties pertaining to StaticWebsites
 */
public final class StaticWebsiteProperties {

    /**
     * Indicates whether static websites are enabled for this account.
     */
    private boolean enabled;

    /**
     * The name of the index document in each directory.
     */
    private String indexDocument;

    /**
     * The path to the error document that should be shown when a 404 is issued.
     */
    private String errorDocument404Path;

    public StaticWebsiteProperties() {
        this.enabled = false;
    }

    /**
     * @return
     *      Whether static websites are enabled on this account.
     */
    public boolean getEnabled() {
        return enabled;
    }

    /**
     * @param enabled indicates whether static websites is enabled.
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * @return
     *      The name of the index document in each directory.
     */
    public String getIndexDocument() {
        return indexDocument;
    }

    /**
     * @param indexDocument
     *      The name of the index document in each directory.
     */
    public void setIndexDocument(String indexDocument) {
        this.indexDocument = indexDocument;
    }

    /**
     * @return
     *      The path to the error document that should be shown when a 404 is issued.
     */
    public String getErrorDocument404Path() {
        return errorDocument404Path;
    }

    /**
     * @param errorDocument404Path
     *      The path to the error document that should be shown when a 404 is issued.
     */
    public void setErrorDocument404Path(String errorDocument404Path) {
        this.errorDocument404Path = errorDocument404Path;
    }
}
