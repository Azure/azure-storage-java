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

import java.net.URL;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * A class used to conveniently parse URLs into {@link QueueURLParts} to modify the components of the URL.
 */
public final class URLParser {

    // ipv4PatternString represents the pattern of ipv4 addresses.
    private final static String ipv4PatternString = "^((0|1\\d?\\d?|2[0-4]?\\d?|25[0-5]?|[3-9]\\d?)\\.){3}(0|1\\d?\\d?|2[0-4]?\\d?|25[0-5]?|[3-9]\\d?)$";
    // ipv4Pattern represents a compiled pattern of ipv4 address pattern.
    private static Pattern ipv4Pattern = Pattern.compile(ipv4PatternString);

    /**
     * URLParser parses a URL initializing QueueURLParts' fields including any SAS-related query parameters.
     * Any other query parameters remain in the UnparsedParams field. This method overwrites all fields in the
     * QueueURLParts object.
     *
     * @param url
     *         The {@code URL} to be parsed.
     *
     * @return A {@link QueueURLParts} object containing all the components of a QueueURL.
     *
     * @throws UnknownHostException
     *         If the url contains an improperly formatted ip address or unknown host address.
     */
    public static QueueURLParts parse(URL url) throws UnknownHostException {

        final String scheme = url.getProtocol();
        final String host = url.getHost();

        String queueName = null;
        boolean messages = false;
        String messageId = null;
        IPStyleEndPointInfo ipStyleEndPointInfo = null;
        Integer port = null;

        String path = url.getPath();
        if (!Utility.isNullOrEmpty(path)) {
            // if the path starts with a slash remove it
            if (path.charAt(0) == '/') {
                path = path.substring(1);
            }

            // If the host name is in the ip-address format
            if (isHostIPEndPointStyle(host)) {
                // Create the IPStyleEndPointInfo and set the port number.
                ipStyleEndPointInfo = new IPStyleEndPointInfo();
                if (url.getPort() != -1) {
                    ipStyleEndPointInfo.withPort(url.getPort());
                }
                String accountName;
                /*If the host is in the IP Format, then account name is provided after the ip-address.
                  For Example: "https://10.132.141.33/accountname/queueame". Get the index of "/" in the path.
                 */
                int pathSepEndIndex = path.indexOf('/');
                if (pathSepEndIndex == -1) {
                    /* If there does not exists "/" in the path, it means the entire path is the account name
                     For Example: path = accountname */
                    accountName = path;
                    // since path contains only the account name, after account name is set, set path to empty string.
                    path = Constants.EMPTY_STRING;
                } else {
                    /* If there exists the "/", it means all the content in the path till "/" is the account name
                     For Example: accountname/queuename/messages */
                    accountName = path.substring(0, pathSepEndIndex);
                    // After path name has been extracted from the path, strip account name from path
                    path = path.substring(pathSepEndIndex + 1);
                }
                ipStyleEndPointInfo.withAccountName(accountName);
            }

            int pathSepEndIndex = path.indexOf('/');
            if (pathSepEndIndex == -1) {
                // path contains only a queue name and no messages / messageId
                queueName = path;
            } else {
                // path contains the queue name up until the slash and messages / messageId is everything after the slash
                queueName = path.substring(0, pathSepEndIndex);
                // Since there exists some content in path string after the queueName,
                // we expect the content after the queueName to be in format "messages/<messageId>"
                // Note: We don't check the keyword "messages" in the path string.
                String messagesWithMessagesId = path.substring(pathSepEndIndex + 1);
                // Set the messages to true since there is some content in path string after after the queueName
                messages = true;
                // Check the pathSeparator in the content after the queueName i.e in "messages/<messageId>"
                // All the content after the path separator will be the messageId
                // If there is no path separator, then messageId is not provided.
                pathSepEndIndex = messagesWithMessagesId.indexOf('/');
                if (pathSepEndIndex != -1) {
                    messageId = messagesWithMessagesId.substring(pathSepEndIndex + 1);
                }
            }
        }
        Map<String, String[]> queryParamsMap = parseQueryString(url.getQuery());

        SASQueryParameters sasQueryParameters = new SASQueryParameters(queryParamsMap, true);

        return new QueueURLParts()
                .withScheme(scheme)
                .withHost(host)
                .withQueueName(queueName)
                .withMessages(messages)
                .withMessageId(messageId)
                .withSasQueryParameters(sasQueryParameters)
                .withUnparsedParameters(queryParamsMap)
                .withIPEndPointStyleInfo(ipStyleEndPointInfo);
    }

    /**
     * Parses a query string into a one to many hashmap.
     *
     * @param queryParams
     *         The string of query params to parse.
     *
     * @return A {@code HashMap<String, String[]>} of the key values.
     */
    private static TreeMap<String, String[]> parseQueryString(String queryParams) {

        final TreeMap<String, String[]> retVals = new TreeMap<String, String[]>(new Comparator<String>() {
            @Override
            public int compare(String s1, String s2) {
                return s1.compareTo(s2);
            }
        });

        if (Utility.isNullOrEmpty(queryParams)) {
            return retVals;
        }

        // split name value pairs by splitting on the 'c&' character
        final String[] valuePairs = queryParams.split("&");

        // for each field value pair parse into appropriate map entries
        for (int m = 0; m < valuePairs.length; m++) {
            // Getting key and value for a single query parameter
            final int equalDex = valuePairs[m].indexOf("=");
            String key = Utility.safeURLDecode(valuePairs[m].substring(0, equalDex)).toLowerCase(Locale.ROOT);
            String value = Utility.safeURLDecode(valuePairs[m].substring(equalDex + 1));

            // add to map
            String[] keyValues = retVals.get(key);

            // check if map already contains key
            if (keyValues == null) {
                // map does not contain this key
                keyValues = new String[]{value};
                retVals.put(key, keyValues);
            } else {
                // map contains this key already so append
                final String[] newValues = new String[keyValues.length + 1];
                for (int j = 0; j < keyValues.length; j++) {
                    newValues[j] = keyValues[j];
                }

                newValues[newValues.length - 1] = value;
            }
        }

        return retVals;
    }

    /**
     * Checks if URL's host is IP. If true storage account endpoint will be composed as: http(s)://IP(:port)/storageaccount/queue(share||container||etc)/...
     * As url's Host property, host could be both host or host:port
     *
     * @param host
     *         The host. Ex: "account.queue.core.windows.net" or 10.31.141.33:80
     *
     * @return true, if the host is an ip address (ipv4/ipv6) with or without port.
     */
    public static boolean isHostIPEndPointStyle(String host) {
        if ((host == null) || host.equals("")) {
            return false;
        }
        // returns true if host represents an ipv4 address.
        return ipv4Pattern.matcher(host).matches();
    }
}
