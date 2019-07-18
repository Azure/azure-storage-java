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

import java.util.Date;
import java.util.EnumSet;

import com.microsoft.azure.storage.AccessCondition;

/**
 * Represents the system properties for a directory.
 */
public final class FileDirectoryProperties {

    /**
     * Represents the ETag value for the directory.
     */
    private String etag;

    /**
     * Represents the directory's last-modified time.
     */
    private Date lastModified;

    /**
     * Represents the directory's server-side encryption status.
     */
    private boolean serverEncrypted;

    /**
     * Represents the directory's permission key.
     */
    private String filePermissionKey;

    /**
     * Represents the file system attributes for files and directories.
     * If not set, indicates preservation of existing values.
     */
    private EnumSet<NtfsAttributes> ntfsAttributes;

    /**
     * Represents the creation time for the directory.
     */
    private String creationTime;

    /**
     * Represents the last-write time for the directory.
     */
    private String lastWriteTime;

    /**
     * Represents the change time for the directory.
     */
    private String changeTime;

    /**
     * Represents the directory's id.
     */
    private String fileId;

    /**
     * Represents the directory's parent id.
     */
    private String parentId;

    /**
     * Gets the ETag value of the directory.
     * <p>
     * The ETag value is a unique identifier that is updated when a write operation is performed against the directory.
     * It may be used to perform operations conditionally, providing concurrency control and improved efficiency.
     * <p>
     * The {@link AccessCondition#generateIfMatchCondition(String)} and
     * {@link AccessCondition#generateIfNoneMatchCondition(String)} methods take an ETag value and return an
     * {@link AccessCondition} object that may be specified on the request.
     * 
     * @return A <code>String</code> which represents the ETag.
     */
    public String getEtag() {
        return this.etag;
    }

    /**
     * Gets the last modified time on the directory.
     * 
     * @return A <code>java.util.Date</code> object which represents the last modified time.
     */
    public Date getLastModified() {
        return this.lastModified;
    }

    /**
     * Gets the directory's server-side encryption status.
     * 
     * @return A <code>boolean</code> which specifies the directory's encryption status.
     */
    public boolean isServerEncrypted() {
        return serverEncrypted;
    }

    /**
     * Gets the directory's permission key.
     *
     * @return A <code>String</code> which specifies the directory's permission key.
     */
    public String getFilePermissionKey() {
        return this.filePermissionKey;
    }

    /**
     * Gets the file system attributes for files and directories.
     * If not set, indicates preservation of existing values.
     *
     * @return A {@link NtfsAttributes} object which represents the file system attributes.
     */
    public EnumSet<NtfsAttributes> getNtfsAttributes() {
        return this.ntfsAttributes;
    }

    /**
     * Gets the creation time for the directory.
     *
     * @return A <code>String</code> object which represents the creation time.
     */
    public String getCreationTime() {
        return this.creationTime;
    }

    /**
     * Gets the last write time for the directory.
     *
     * @return A <code>String</code> object which represents the last write time.
     */
    public String getLastWriteTime() {
        return this.lastWriteTime;
    }

    /**
     * Gets the change time for the directory.
     *
     * @return A <code>String</code> object which represents the change time.
     */
    public String getChangeTime() {
        return this.changeTime;
    }

    /**
     * Gets the directory's id.
     *
     * @return A <code>String</code> which specifies the directory's id.
     */
    public String getFileId() {
        return this.fileId;
    }

    /**
     * Gets the directory's parent id.
     *
     * @return A <code>String</code> which specifies the directory's parent id.
     */
    public String getParentId() {
        return this.parentId;
    }

    /**
     * Sets the ETag value on the directory.
     * 
     * @param etag
     *            A <code>String</code> which represents the ETag to set.
     */
    protected void setEtag(final String etag) {
        this.etag = etag;
    }

    /**
     * Sets the directory's server-side encryption status.
     * 
     * @param serverEncrypted
     *        A <code>boolean</code> which specifies the encryption status to set.
     */
    protected void setServerEncrypted(boolean serverEncrypted) {
        this.serverEncrypted = serverEncrypted;
    }

    /**
     * Sets the last modified time on the directory.
     * 
     * @param lastModified
     *            A <code>java.util.Date</code> object which represents the last modified time to set.
     */
    protected void setLastModified(final Date lastModified) {
        this.lastModified = lastModified;
    }

    /**
     * Sets the directory's permission key.
     *
     * @param filePermissionKey
     *        A <code>String</code> which specifies the directory permission key to set.
     */
    public void setFilePermissionKey(String filePermissionKey) {
        this.filePermissionKey = filePermissionKey;
    }

    /**
     * Sets the file system attributes for files and directories.
     * If not set, indicates preservation of existing values.
     *
     * @param ntfsAttributes
     *        A {@link NtfsAttributes} which specifies the file system attributes to set.
     */
    public void setNtfsAttributes(EnumSet<NtfsAttributes> ntfsAttributes) {
        this.ntfsAttributes = ntfsAttributes;
    }

    /**
     * Sets the creation time for the directory.
     *
     * @param creationTime
     *            A <code>String</code> object which specifies the creation time to set.
     */
    public void setCreationTime(String creationTime) {
        this.creationTime = creationTime;
    }

    /**
     * Sets the last write time for the directory.
     *
     * @param lastWriteTime
     *            A <code>String</code> object which specifies the last write time to set.
     */
    public void setLastWriteTime(String lastWriteTime) {
        this.lastWriteTime = lastWriteTime;
    }

    /**
     * Sets the change time for the directory.
     *
     * @param changeTime
     *            A <code>String</code> object which specifies the change time to set.
     */
    protected void setChangeTime(String changeTime) {
        this.changeTime = changeTime;
    }

    /**
     * Sets the directory's id.
     *
     * @param fileId
     *        A <code>String</code> which specifies the id to set.
     */
    protected void setFileId(String fileId) {
        this.fileId = fileId;
    }

    /**
     * Sets the directory's parent id.
     *
     * @param parentId
     *        A <code>String</code> which specifies the parent id to set.
     */
    protected void setParentId(String parentId) {
        this.parentId = parentId;
    }
}
