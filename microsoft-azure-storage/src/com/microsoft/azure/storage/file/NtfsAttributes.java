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
/**
 * 
 */
package com.microsoft.azure.storage.file;

/**
 * Specifies options for NTFS Attributes.
 */
public enum NtfsAttributes {

    /**
     * The file is read-only.
     */
    READ_ONLY,

    /**
     * The file is hidden, and thus is not included in ordinary directory listing.
     */
    HIDDEN,

    /**
     * The file is a system file. That is, the file is part of the operating system
     * or is used exclusively by the operating system.
     */
    SYSTEM,

    /**
     * The file is a standard file that has no special attributes.
     */
    NORMAL,

    /**
     * The file is a directory.
     */
    DIRECTORY,

    /**
     * The file is a candidate for backup or removal.
     */
    ARCHIVE,

    /**
     * The file is temporary. A temporary file contains data that is needed while an
     * application is executing but is not needed after the application is finished.
     * File systems try to keep all the data in memory for quicker access rather than
     * flushing the data back to mass storage. A temporary file should be deleted by
     * the application as soon as it is no longer needed.
     */
    TEMPORARY,

    /**
     * The file is offline. The data of the file is not immediately available.
     */
    OFFLINE,

    /**
     * The file will not be indexed by the operating system's content indexing service.
     */
    NOT_CONTENT_INDEXED,

    /**
     * The file or directory is excluded from the data integrity scan. When this value
     * is applied to a directory, by default, all new files and subdirectories within
     * that directory are excluded from data integrity.
     */
    NO_SCRUB_DATA

}
