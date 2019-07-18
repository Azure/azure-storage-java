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

import com.microsoft.azure.storage.Constants;

import java.util.EnumSet;

/**
 * Parses NTFS Attributes.
 */
class NtfsAttributesParser {

    /**
     * Converts an enum set of {@code NtfsAttributes} to a string.
     *
     * @return A <code>String</code> that represents the NTFS Attributes in the correct format delimited by |
     *         which is described at {@link #toAttributes(String)}.
     */
    public static String toString(EnumSet<NtfsAttributes> ntfsAttributes) {
        if (ntfsAttributes == null) {
            return Constants.EMPTY_STRING;
        }

        final StringBuilder builder = new StringBuilder();

        if (ntfsAttributes.contains(NtfsAttributes.READ_ONLY)) {
            builder.append("ReadOnly|");
        }

        if (ntfsAttributes.contains(NtfsAttributes.HIDDEN)) {
            builder.append("Hidden|");
        }

        if (ntfsAttributes.contains(NtfsAttributes.SYSTEM)) {
            builder.append("System|");
        }

        if (ntfsAttributes.contains(NtfsAttributes.NORMAL)) {
            builder.append("None|");
        }

        if (ntfsAttributes.contains(NtfsAttributes.DIRECTORY)) {
            builder.append("Directory|");
        }

        if (ntfsAttributes.contains(NtfsAttributes.ARCHIVE)) {
            builder.append("Archive|");
        }

        if (ntfsAttributes.contains(NtfsAttributes.TEMPORARY)) {
            builder.append("Temporary|");
        }

        if (ntfsAttributes.contains(NtfsAttributes.OFFLINE)) {
            builder.append("Offline|");
        }

        if (ntfsAttributes.contains(NtfsAttributes.NOT_CONTENT_INDEXED)) {
            builder.append("NotContentIndexed|");
        }

        if (ntfsAttributes.contains(NtfsAttributes.NO_SCRUB_DATA)) {
            builder.append("NoScrubData|");
        }

        builder.deleteCharAt(builder.lastIndexOf("|"));

        return builder.toString();
    }

    /**
     * Creates an enum set of {@code NtfsAttributes} from a valid String .
     *
     * @param ntfsAttributes
     *            A <code>String</code> that represents the ntfs attributes. The string must contain one or
     *            more of the following values delimited by a |. Note they are case sensitive.
     *            <ul>
     *            <li><code>ReadOnly</code></li>
     *            <li><code>Hidden</code></li>
     *            <li><code>System</code></li>
     *            <li><code>None</code></li>
     *            <li><code>Directory</code></li>
     *            <li><code>Archive</code></li>
     *            <li><code>Temporary</code></li>
     *            <li><code>Offline</code></li>
     *            <li><code>NotContentIndexed</code></li>
     *            <li><code>NoScrubData</code></li>
     *            </ul>
     */
    public static EnumSet<NtfsAttributes> toAttributes(String ntfsAttributes) {
        EnumSet<NtfsAttributes> attributes = EnumSet.noneOf(NtfsAttributes.class);
        String[] splitAttributes = ntfsAttributes.split("\\|");

        for(String sa: splitAttributes) {
           if (sa.equals("ReadOnly")) {
              attributes.add(NtfsAttributes.READ_ONLY);
           } else if (sa.equals("Hidden")) {
               attributes.add(NtfsAttributes.HIDDEN);
           } else if (sa.equals("System")) {
               attributes.add(NtfsAttributes.SYSTEM);
           } else if (sa.equals("None")) {
               attributes.add(NtfsAttributes.NORMAL);
           } else if (sa.equals("Directory")) {
               attributes.add(NtfsAttributes.DIRECTORY);
           } else if (sa.equals("Archive")) {
               attributes.add(NtfsAttributes.ARCHIVE);
           } else if (sa.equals("Temporary")) {
               attributes.add(NtfsAttributes.TEMPORARY);
           } else if (sa.equals("Offline")) {
               attributes.add(NtfsAttributes.OFFLINE);
           } else if (sa.equals("NotContentIndexed")) {
               attributes.add(NtfsAttributes.NOT_CONTENT_INDEXED);
           } else if (sa.equals("NoScrubData")) {
               attributes.add(NtfsAttributes.NO_SCRUB_DATA);
           } else {
               throw new IllegalArgumentException("value");
           }
        }

        return attributes;
    }

}
