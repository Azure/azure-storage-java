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
 * Holds information related to the storage account.
 */
public final class AccountInformation {

    private String skuName;

    private String accountKind;

    /**
     * @return
     *      The name of the storage SKU, also known as account type.
     *      Example: Standard_LRS, Standard_GRS, Standard_RAGRS, Premium_LRS, Premium_ZRS
     */
    public String getSkuName() {
        return this.skuName;
    }

    /**

     * @return
     *      Describes the flavour of the storage account, also known as account kind.
     *      Example: Storage, StorageV2, BlobStorage
     */
    public String getAccountKind() {
        return this.accountKind;
    }

    public void setSkuName(String skuName) {
        this.skuName = skuName;
    }

    public void setAccountKind(String accountKind) {
        this.accountKind = accountKind;
    }
}
