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

import com.microsoft.azure.storage.Constants;
import com.microsoft.azure.storage.core.Utility;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.Date;

final public class UserDelegationKeyRequestSerializer {

    public static byte[] writeKeyRequestToStream(final Date keyStart, final Date keyEnd) throws XMLStreamException, UnsupportedEncodingException {
        final StringWriter outWriter = new StringWriter();
        final XMLStreamWriter xmlw;

        xmlw = Utility.createXMLStreamWriter(outWriter);
        xmlw.writeStartDocument();
        xmlw.writeStartElement("KeyInfo");
        xmlw.writeStartElement("Start");
        xmlw.writeCharacters(Utility.getUTCTimeOrEmpty(keyStart));
        xmlw.writeEndElement();
        xmlw.writeStartElement("Expiry");
        xmlw.writeCharacters(Utility.getUTCTimeOrEmpty(keyEnd));
        xmlw.writeEndElement();
        xmlw.writeEndElement();
        xmlw.writeEndDocument();

        return outWriter.toString().getBytes(Constants.UTF8_CHARSET);
    }
}
