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
package com.microsoft.azure.storage.blob;

import com.microsoft.azure.storage.SharedAccessHeaders;

/**
 * Represents the optional headers that can be returned with blobs accessed using SAS.
 */
public final class SharedAccessBlobHeaders extends SharedAccessHeaders {
    /**
     * Initializes a new instance of the {@link SharedAccessBlobHeaders} class.
     */
    public SharedAccessBlobHeaders() {
        super();
    }

    /**
     * Initializes a new instance of the {@link SharedAccessHeaders} class. The empty constructor should be preferred
     * and this option should only be used by customers who are sure they do not want the safety usually afforded by
     * this SDK when constructing a sas.
     * <p>
     * The header values are typically decoded before building the sas token. This can cause problems if the desired
     * value for one of the headers contains something that looks like encoding. Setting this flag to true will ensure
     * that the value of these headers are preserved as set on this object when constructing the sas.
     * <p>
     * Note that these values are preserved by encoding them here so that the decoding which happens at sas construction
     * time returns them to the original values. So if get is called on this object when preserveRawValues was set to
     * true, the value returned will be percent encoded.
     *
     * @param preserveRawValue Whether the sdk should preserve the raw value of these headers.
     */
    public SharedAccessBlobHeaders(boolean preserveRawValue) {
        super(preserveRawValue);
    }
    /**
     * Initializes a new instance of the {@link SharedAccessBlobHeaders} class based on an existing instance.
     * 
     * @param other
     *            A {@link SharedAccessHeaders} object which specifies the set of properties to clone.
     */
    public SharedAccessBlobHeaders(SharedAccessHeaders other) {
        super(other);
    }
}