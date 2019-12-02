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

import com.microsoft.azure.storage.core.Utility;

/**
 *  RESERVED FOR INTERNAL USE. Represents the optional headers that can be returned using SAS.
 */
public abstract class SharedAccessHeaders {
    private final boolean preserveRawValue;

    /**
     * The cache-control header returned.
     */
    private String cacheControl;

    /**
     * The content-disposition header returned.
     */
    private String contentDisposition;

    /**
     * The content-encoding header returned.
     */
    private String contentEncoding;

    /**
     * The content-language header returned.
     */
    private String contentLanguage;

    /**
     * The content-type header returned.
     */
    private String contentType;

    /**
     * Initializes a new instance of the {@link SharedAccessHeaders} class.
     */
    public SharedAccessHeaders() {
        this(false);
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
    public SharedAccessHeaders(boolean preserveRawValue) {
        this.preserveRawValue = preserveRawValue;
    }

    /**
     * Initializes a new instance of the {@link SharedAccessHeaders} class based on an existing instance.
     *
     * @param other
     *            A {@link SharedAccessHeaders} object which specifies the set of properties to clone.
     */
    public SharedAccessHeaders(SharedAccessHeaders other) {
        Utility.assertNotNull("other", other);

        this.contentType = other.contentType;
        this.contentDisposition = other.contentDisposition;
        this.contentEncoding = other.contentEncoding;
        this.contentLanguage = other.contentLanguage;
        this.cacheControl = other.cacheControl;

        this.preserveRawValue = other.preserveRawValue;
    }

    /**
     * Gets the cache control header.
     *
     * @return A <code>String</code> which represents the cache control header.
     */
    public String getCacheControl() {
        return this.cacheControl;
    }

    /**
     * Sets the cache control header.
     *
     * @param cacheControl
     *            A <code>String</code> which specifies the cache control header.
     */
    public void setCacheControl(String cacheControl) {
        if (this.preserveRawValue) {
            try {
                this.cacheControl = Utility.safeEncode(cacheControl);
            } catch (StorageException e) {
                e.printStackTrace();
            }
        }
        else {
            this.cacheControl = cacheControl;
        }
    }

    /**
     * Gets the content disposition header.
     *
     * @return A <code>String</code> which represents the content disposition header.
     */
    public String getContentDisposition() {
        return this.contentDisposition;
    }

    /**
     * Sets the content disposition header.
     *
     * @param contentDisposition
     *            A <code>String</code> which specifies the content disposition header.
     */
    public void setContentDisposition(String contentDisposition) {
        if (this.preserveRawValue) {
            try {
                this.contentDisposition = Utility.safeEncode(contentDisposition);
            } catch (StorageException e) {
                e.printStackTrace();
            }
        }
        else {
            this.contentDisposition = contentDisposition;
        }
    }

    /**
     * Gets the content encoding header.
     *
     * @return A <code>String</code> which represents the content encoding header.
     */
    public String getContentEncoding() {
        return this.contentEncoding;
    }

    /**
     * Sets the content encoding header.
     *
     * @param contentEncoding
     *            A <code>String</code> which specifies the content encoding header.
     */
    public void setContentEncoding(String contentEncoding) {
        if (this.preserveRawValue) {
            try {
                this.contentEncoding = Utility.safeEncode(contentEncoding);
            } catch (StorageException e) {
                e.printStackTrace();
            }
        }
        else {
            this.contentEncoding = contentEncoding;
        }
    }

    /**
     * Gets the content language header.
     *
     * @return A <code>String</code> which represents the content language header.
     */
    public String getContentLanguage() {
        return this.contentLanguage;
    }

    /**
     * Sets the content language header.
     *
     * @param contentLanguage
     *            A <code>String</code> which specifies the content language header.
     */
    public void setContentLanguage(String contentLanguage) {
        if (this.preserveRawValue) {
            try {
                this.contentLanguage = Utility.safeEncode(contentLanguage);
            } catch (StorageException e) {
                e.printStackTrace();
            }
        }
        else {
            this.contentLanguage = contentLanguage;
        }
    }

    /**
     * Gets the content type header.
     *
     * @return A <code>String</code> which represents the content type header.
     */
    public String getContentType() {
        return this.contentType;
    }

    /**
     * Sets the content type header.
     *
     * @param contentType
     *            A <code>String</code> which specifies the content type header.
     */
    public void setContentType(String contentType) {
        if (this.preserveRawValue) {
            try {
                this.contentType = Utility.safeEncode(contentType);
            } catch (StorageException e) {
                e.printStackTrace();
            }
        }
        else {
            this.contentType = contentType;
        }
    }
}

