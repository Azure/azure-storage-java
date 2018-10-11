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
package com.microsoft.azure.storage.queue;

import com.microsoft.rest.v2.http.UrlBuilder;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;


/**
 * A QueueURLParts object represents the components that make up an Azure Storage Queue/QueueMessage URL. You may parse an
 * existing URL into its parts with the {@link URLParser} class. You may construct a URL from parts by calling toURL().
 * It is also possible to use the empty constructor to build a queueURL from scratch.
 * NOTE: Changing any SAS-related field requires computing a new SAS signature.
 */
public final class QueueURLParts {

    private String scheme;

    private String host;

    private String queueName;

    private boolean messages;

    private String messageId;

    private SASQueryParameters sasQueryParameters;

    private Map<String, String[]> unparsedParameters;

    private IPStyleEndPointInfo ipStyleEndPointInfo;

    /**
     * The scheme. Ex: "https://".
     */
    public String scheme() {
        return scheme;
    }

    /**
     * The scheme. Ex: "https://".
     */
    public QueueURLParts withScheme(String scheme) {
        this.scheme = scheme;
        return this;
    }

    /**
     * The host. Ex: "account.queue.core.windows.net".
     */
    public String host() {
        return host;
    }

    /**
     * The host. Ex: "account.queue.core.windows.net".
     */
    public QueueURLParts withHost(String host) {
        this.host = host;
        return this;
    }

    /**
     * The queueName name or {@code null} if a {@link ServiceURL} was parsed.
     */
    public String queueName() {
        return queueName;
    }

    /**
     * The queueName name or {@code null} if a {@link ServiceURL} was parsed.
     */
    public QueueURLParts withQueueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    /**
     * Messages is true if "/messages" was/should be in URL
     */
    public boolean messages() {
        return messages;
    }

    /**
     * Messages is true if "/messages" was/should be in URL
     */
    public QueueURLParts withMessages(boolean messages) {
        this.messages = messages;
        return this;
    }

    /**
     * MessageId represents the messageID of the message represented by the URL.
     */
    public String messageId() {
        return messageId;
    }

    /**
     * MessageId represents the messageID of the message represented by the URL.
     */
    public QueueURLParts withMessageId(String messageId) {
        this.messageId = messageId;
        return this;
    }

    /**
     * A {@link SASQueryParameters} representing the SAS query parameters or {@code null} if there were no such
     * parameters.
     */
    public SASQueryParameters sasQueryParameters() {
        return sasQueryParameters;
    }

    /**
     * A {@link SASQueryParameters} representing the SAS query parameters or {@code null} if there were no such
     * parameters.
     */
    public QueueURLParts withSasQueryParameters(SASQueryParameters sasQueryParameters) {
        this.sasQueryParameters = sasQueryParameters;
        return this;
    }

    /**
     * A {@link IPStyleEndPointInfo}
     */
    public IPStyleEndPointInfo ipEndPointStyleInfo() {
        return ipStyleEndPointInfo;
    }

    /**
     * A {@link IPStyleEndPointInfo}
     */
    public QueueURLParts withIPEndPointStyleInfo(IPStyleEndPointInfo ipStyleEndPointInfo) {
        this.ipStyleEndPointInfo = ipStyleEndPointInfo;
        return this;
    }

    /**
     * The query parameter key value pairs aside from SAS parameters or {@code null} if there were
     * no such parameters.
     */
    public Map<String, String[]> unparsedParameters() {
        return unparsedParameters;
    }

    /**
     * The query parameter key value pairs aside from SAS parameters or {@code null} if there were
     * no such parameters.
     */
    public QueueURLParts withUnparsedParameters(Map<String, String[]> unparsedParameters) {
        this.unparsedParameters = unparsedParameters;
        return this;
    }

    /**
     * Initializes a QueueURLParts object with all fields set to null, except unparsedParameters, which is an empty map.
     * This may be useful for constructing a URL to a queue storage resource from scratch when the constituent parts are
     * already known.
     */
    public QueueURLParts() {
        unparsedParameters = new HashMap<>();
    }

    /**
     * Converts the queue URL parts to a {@link URL}.
     *
     * @return A {@code java.net.URL} to the queue resource composed of all the elements in the object.
     *
     * @throws MalformedURLException
     *         The fields present on the QueueURLParts object were insufficient to construct a valid URL or were
     *         ill-formatted.
     */
    public URL toURL() throws MalformedURLException {
        UrlBuilder url = new UrlBuilder().withScheme(this.scheme).withHost(this.host);

        StringBuilder path = new StringBuilder();
        if (this.messagesIdSpecified() && (!this.messages || !this.queueNameSpecified())) {
            throw new MalformedURLException("Cannot produce a URL with a messageId but without queue name or messages.");
        }

        if (this.messages && !this.queueNameSpecified()) {
            throw new MalformedURLException("Cannot produce a URL with Messages but without a queue name.");
        }

        if (this.ipStyleEndPointInfo != null) {
            if (this.ipStyleEndPointInfo.accountName() != null) {
                /* Added a path separator after the account name. Anything that is added
                 after the account name doesn't need to care about the "/". */
                path.append(this.ipStyleEndPointInfo.accountName() + "/");
            }
            if (this.ipStyleEndPointInfo.port() != null) {
                url.withPort(this.ipStyleEndPointInfo.port());
            }
        }

        // Concatenate queue name (if it exists)
        if (this.queueNameSpecified()) {
            path.append(this.queueName);
            if (this.messages) {
                // If messages is set to true, append "messages" keyword to the Url.
                path.append("/messages");
                // Concatenate messageId (if it exists)
                if (this.messagesIdSpecified()) {
                    path.append("/");
                    path.append(this.messageId);
                }
            }
        }

        url.withPath(path.toString());

        if (this.sasQueryParameters != null) {
            String encodedSAS = this.sasQueryParameters.encode();
            if (encodedSAS.length() != 0) {
                url.withQuery(encodedSAS);
            }
        }

        for (Map.Entry<String, String[]> entry : this.unparsedParameters.entrySet()) {
            // The commas are intentionally encoded.
            url.setQueryParameter(entry.getKey(),
                    Utility.safeURLEncode(String.join(",", entry.getValue())));
        }
        return url.toURL();
    }

    // messagesIdSpecified function returns whether there exists a messageId in the QueueURLParts or not.
    private boolean messagesIdSpecified() {
        if (this.messageId != null && this.messageId.length() > 0)
            return true;
        return false;
    }

    // queueNameSpecified function returns whether there exists a queue name in the QueueURLParts or not.
    private boolean queueNameSpecified() {
        if (this.queueName != null && this.queueName.length() > 0) {
            return true;
        }
        return false;
    }
}
