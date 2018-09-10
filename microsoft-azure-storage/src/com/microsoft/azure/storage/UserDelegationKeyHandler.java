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

import com.microsoft.azure.storage.core.SR;
import com.microsoft.azure.storage.core.Utility;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import java.io.IOException;
import java.io.InputStream;
import java.util.Stack;
import java.util.UUID;

public class UserDelegationKeyHandler extends DefaultHandler {

    private final Stack<String> elementStack = new Stack<>();
    private final UserDelegationKey key = new UserDelegationKey();

    private StringBuilder sb = new StringBuilder();

    public static UserDelegationKey readUserDelegationKeyFromStream(final InputStream stream)
            throws SAXException, IOException, ParserConfigurationException {
        SAXParser saxParser = Utility.getSAXParser();
        UserDelegationKeyHandler handler = new UserDelegationKeyHandler();
        saxParser.parse(stream, handler);

        return handler.key;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        this.elementStack.push(localName);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        String currentNode = this.elementStack.pop();

        // if the node popped from the stack and the localName don't match, the xml document is improperly formatted
        if (!localName.equals(currentNode)) {
            throw new SAXException(SR.INVALID_RESPONSE_RECEIVED);
        }

        String value = this.sb.toString();
        sb = new StringBuilder();
        if (value.isEmpty()) {
            value = null;
        }

        switch (localName) {
            case Constants.SIGNED_OID_ELEMENT:
                try {
                    key.setSignedOid(UUID.fromString(value));
                }
                catch (Exception e) {
                    // no-op
                }
                break;

            case Constants.SIGNED_TID_ELEMENT:
                try {
                    key.setSignedTid(UUID.fromString(value));
                }
                catch (Exception e) {
                    // no-op
                }
                break;

            case Constants.SIGNED_START_ELEMENT:
                try {
                    key.setSignedStart(Utility.parseDate(value));
                }
                catch (Exception e) {
                    // no-op
                }
                break;

            case Constants.SIGNED_EXPIRY_ELEMENT:
                try {
                    key.setSignedExpiry(Utility.parseDate(value));
                }
                catch (Exception e) {
                    // no-op
                }
                break;

            case Constants.SIGNED_VERSION_ELEMENT:
                key.setSignedVersion(value);
                break;

            case Constants.SIGNED_SERVICE_ELEMENT:
                key.setSignedService(value);
                break;

            case Constants.VALUE_ELEMENT:
                key.setValue(value);
                break;

            default:
                break;
        }
    }

    @Override
    public void characters(char ch[], int start, int length) throws SAXException {
        this.sb.append(ch, start, length);
    }
}
