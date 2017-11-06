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
package com.microsoft.azure.storage.core;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;
import java.util.Map.Entry;

import com.microsoft.azure.storage.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamException;

/**
 * RESERVED FOR INTERNAL USE. The Base Request class for the protocol layer.
 */
public final class BaseRequest {
    public static final String METADATA = "metadata";

    public static final String SERVICE = "service";

    public static final String STATS = "stats";

    public static final String TIMEOUT = "timeout";

    /**
     * Adds the metadata.
     * 
     * @param request
     *            The request.
     * @param metadata
     *            The metadata.
     */
    public static void addMetadata(final HttpURLConnection request, final Map<String, String> metadata,
            final OperationContext opContext) {
        if (metadata != null) {
            for (final Entry<String, String> entry : metadata.entrySet()) {
                addMetadata(request, entry.getKey(), entry.getValue(), opContext);
            }
        }
    }

    /**
     * Adds the metadata.
     * 
     * @param opContext
     *            an object used to track the execution of the operation
     * @param request
     *            The request.
     * @param name
     *            The metadata name.
     * @param value
     *            The metadata value.
     */
    private static void addMetadata(final HttpURLConnection request, final String name, final String value,
            final OperationContext opContext) {
        if (Utility.isNullOrEmptyOrWhitespace(name)) {
            throw new IllegalArgumentException(SR.METADATA_KEY_INVALID);
        }
        else if (Utility.isNullOrEmptyOrWhitespace(value)) {
            throw new IllegalArgumentException(SR.METADATA_VALUE_INVALID);
        }

        request.setRequestProperty(Constants.HeaderConstants.PREFIX_FOR_STORAGE_METADATA + name, value);
    }

    /**
     * Adds the optional header.
     * 
     * @param request
     *            a HttpURLConnection for the operation.
     * @param name
     *            the metadata name.
     * @param value
     *            the metadata value.
     */
    public static void addOptionalHeader(final HttpURLConnection request, final String name, final String value) {
        if (value != null && !value.equals(Constants.EMPTY_STRING)) {
            request.setRequestProperty(name, value);
        }
    }

    /**
     * Gets a {@link UriQueryBuilder} for listing.
     * 
     * @param listingContext
     *            A {@link ListingContext} object that specifies parameters for
     *            the listing operation, if any. May be <code>null</code>.
     *            
     * @throws StorageException
     *             If a storage service error occurred during the operation.
     */
    public static UriQueryBuilder getListUriQueryBuilder(final ListingContext listingContext) throws StorageException {
        final UriQueryBuilder builder = new UriQueryBuilder();    
        builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.LIST);

        if (listingContext != null) {
            if (!Utility.isNullOrEmpty(listingContext.getPrefix())) {
                builder.add(Constants.QueryConstants.PREFIX, listingContext.getPrefix());
            }

            if (!Utility.isNullOrEmpty(listingContext.getMarker())) {
                builder.add(Constants.QueryConstants.MARKER, listingContext.getMarker());
            }

            if (listingContext.getMaxResults() != null && listingContext.getMaxResults() > 0) {
                builder.add(Constants.QueryConstants.MAX_RESULTS, listingContext.getMaxResults().toString());
            }
        }

        return builder;
    }

    public static ServiceProperties defaultReadServicePropertiesFromConnection(HttpURLConnection connection)
            throws IOException, ParserConfigurationException, SAXException {
        return ServicePropertiesHandler.readServicePropertiesFromStream(connection.getInputStream());
    }

    public static ServiceStats defaultReadServiceStatsFromConnection(HttpURLConnection connection)
            throws IOException, ParserConfigurationException, SAXException {
        return ServiceStatsHandler.readServiceStatsFromStream(connection.getInputStream());
    }

    public static byte[] defaultSerializeServicePropertiesToByteArray(ServiceProperties properties)
            throws XMLStreamException, StorageException {
        return ServicePropertiesSerializer.serializeToByteArray(properties);
    }

    /**
     * A private default constructor. All methods of this class are static so no instances of it should ever be created.
     */
    private BaseRequest() {
        // No op
    }
}
