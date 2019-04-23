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
package com.microsoft.azure.storage.file;

import com.microsoft.azure.storage.Constants;

/**
 * Holds the Constants used for the File Service.
 */
final class FileConstants {

    /**
     * XML element for a file.
     */
    public static final String FILE_ELEMENT = "File";

    /**
     * XML element for a directory.
     */
    public static final String DIRECTORY_ELEMENT = "Directory";

    /**
     * XML element for a handle.
     */
    public static final String HANDLE_ELEMENT = "Handle";

    /**
     * XML element for a file range.
     */
    public static final String FILE_RANGE_ELEMENT = "Range";

    /**
     * XML element for a share.
     */
    public static final String SHARE_ELEMENT = "Share";

    /**
     * XML element for shares.
     */
    public static final String SHARES_ELEMENT = "Shares";

    /**
     * XML element for share quota.
     */
    public static final String SHARE_QUOTA_ELEMENT = "Quota";

    /**
     * XML element for file range start elements.
     */
    public static final String START_ELEMENT = "Start";

    /**
     * XML element for handle id.
     */
    public static final String HANDLE_ID_ELEMENT = "HandleId";

    /**
     * XML element for path.
     */
    public static final String PATH_ELEMENT = "Path";

    /**
     * XML element for file id.
     */
    public static final String FILE_ID_ELEMENT = "FileId";

    /**
     * XML element for parent id.
     */
    public static final String PARENT_ID_ELEMENT = "ParentId";

    /**
     * XML element for session id.
     */
    public static final String SESSION_ID_ELEMENT = "SessionId";

    /**
     * XML element for client ip.
     */
    public static final String CLIENT_IP_ELEMENT = "ClientIp";

    /**
     * XML element for open time.
     */
    public static final String OPEN_TIME_ELEMENT = "OpenTime";

    /**
     * XML element for handle last reconnect element.
     */
    public static final String LAST_RECONNECT_TIME_ELEMENT = "LastReconnectElement";

    /**
     * The number of default concurrent requests for parallel operation.
     */
    public static final int DEFAULT_CONCURRENT_REQUEST_COUNT = 1;

    /**
     * The largest possible share quota in GB.
     */
    public static final int MAX_SHARE_QUOTA = 5120;

    /**
     * The header that specifies all handles.
     */
    public static final String ALL_HANDLES = "*";

    /**
     * The header that specifies file cache control.
     */
    public static final String CACHE_CONTROL_HEADER = Constants.PREFIX_FOR_STORAGE_HEADER + "cache-control";

    /**
     * The header that specifies file content encoding.
     */
    public static final String CONTENT_DISPOSITION_HEADER = Constants.PREFIX_FOR_STORAGE_HEADER + "content-disposition";

    /**
     * The header that specifies file content encoding.
     */
    public static final String CONTENT_ENCODING_HEADER = Constants.PREFIX_FOR_STORAGE_HEADER + "content-encoding";

    /**
     * The header that specifies file content language.
     */
    public static final String CONTENT_LANGUAGE_HEADER = Constants.PREFIX_FOR_STORAGE_HEADER + "content-language";

    /**
     * The header that specifies file content length.
     */
    public static final String CONTENT_LENGTH_HEADER = Constants.PREFIX_FOR_STORAGE_HEADER + "content-length";

    /**
     * The header that specifies file content type.
     */
    public static final String CONTENT_TYPE_HEADER = Constants.PREFIX_FOR_STORAGE_HEADER + "content-type";

    /**
     * The header that specifies file content MD5.
     */
    public static final String FILE_CONTENT_MD5_HEADER = Constants.PREFIX_FOR_STORAGE_HEADER + "content-md5";

    /**
     * The header for the file type.
     */
    public static final String FILE_TYPE_HEADER = Constants.PREFIX_FOR_STORAGE_HEADER + "type";

    /**
     * Specifies the file type.
     */
    public static final String FILE = "File";

    /**
     * The header that specifies range write mode.
     */
    public static final String FILE_RANGE_WRITE = Constants.PREFIX_FOR_STORAGE_HEADER + "write";

    /**
     * The header that specifies the handle id.
     */
    public static final String HANDLE_ID = Constants.PREFIX_FOR_STORAGE_HEADER + "handle-id";

    /**
     * The header that specifies the number of handles closed.
     */
    public static final String NUMBER_OF_HANDLES_CLOSED = Constants.PREFIX_FOR_STORAGE_HEADER + "number-of-handles-closed";

    /**
     * The header for the share quota.
     */
    public static final String SHARE_QUOTA_HEADER = Constants.PREFIX_FOR_STORAGE_HEADER + "share-quota";

    /**
     * Private Default Constructor.
     */
    private FileConstants() {
        // No op
    }
}
