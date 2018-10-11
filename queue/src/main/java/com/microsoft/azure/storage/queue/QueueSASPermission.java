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


import java.util.Locale;

/**
 * This is a helper class to construct a string representing the permissions granted by a ServiceSAS to a queue.
 * Setting a value to true means that any SAS which uses these permissions will grant permissions for that operation.
 * Once all the values are set, this should be serialized with toString and set as the permissions field on a
 * {@link ServiceSASSignatureValues} object. It is possible to construct the permissions string without this class, but
 * the order of the permissions is particular and this class guarantees correctness.
 */
public final class QueueSASPermission {

    private boolean read;

    private boolean add;

    private boolean update;

    private boolean processMessage;

    /**
     * Specifies Read access granted.
     */
    public boolean read() {
        return read;
    }

    /**
     * Specifies Read access granted.
     */
    public QueueSASPermission withRead(boolean read) {
        this.read = read;
        return this;
    }

    /**
     * Specifies Add access granted.
     */
    public boolean add() {
        return add;
    }

    /**
     * Specifies Add access granted.
     */
    public QueueSASPermission withAdd(boolean add) {
        this.add = add;
        return this;
    }

    /**
     * Specifies Update access granted.
     */
    public boolean update() {
        return update;
    }

    /**
     * Specifies Update access granted.
     */
    public QueueSASPermission withUpdate(boolean update) {
        this.update = update;
        return this;
    }

    /**
     * Specifies Get and Delete access granted.
     */
    public boolean processMessage() {
        return processMessage;
    }

    /**
     * Specifies Get and Delete access granted.
     */
    public QueueSASPermission withProcessMessage(boolean processMessage) {
        this.processMessage = processMessage;
        return this;
    }

    /**
     * Initializes an {@code QueueSASPermission} object with all fields set to false.
     */
    public QueueSASPermission() {
    }


    /**
     * Converts the given permissions to a {@code String}. Using this method will guarantee the permissions are in an
     * order accepted by the service.
     *
     * @return A {@code String} which represents the {@code QueueSASPermission}.
     */
    @Override
    public String toString() {
        // The order of the characters should be as specified here to ensure correctness:
        // https://docs.microsoft.com/en-us/rest/api/storageservices/constructing-a-service-sas
        final StringBuilder builder = new StringBuilder();

        if (this.read) {
            builder.append('r');
        }

        if (this.add) {
            builder.append('a');
        }

        if (this.update) {
            builder.append('u');
        }

        if (this.processMessage) {
            builder.append('p');
        }

        return builder.toString();
    }

    /**
     * Creates a {@code QueueSASPermission} from the specified permissions string. This method will throw an
     * {@code IllegalArgumentException} if it encounters a character that does not correspond to a valid permission.
     *
     * @param permString
     *         A {@code String} which represents the {@code QueueSASPermission}.
     *
     * @return A {@code QueueSASPermission} generated from the given {@code String}.
     */
    public static QueueSASPermission parse(String permString) {
        QueueSASPermission permissions = new QueueSASPermission();

        for (int i = 0; i < permString.length(); i++) {
            char c = permString.charAt(i);
            switch (c) {
                case 'r':
                    permissions.read = true;
                    break;
                case 'a':
                    permissions.add = true;
                    break;
                case 'u':
                    permissions.update = true;
                    break;
                case 'p':
                    permissions.processMessage = true;
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format(Locale.ROOT, SR.ENUM_COULD_NOT_BE_PARSED_INVALID_VALUE, "Permissions", permString, c));
            }
        }
        return permissions;
    }
}