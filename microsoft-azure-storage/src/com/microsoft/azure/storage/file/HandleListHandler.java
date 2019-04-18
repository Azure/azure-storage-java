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

package com.microsoft.azure.storage.file;

import com.microsoft.azure.storage.Constants;
import com.microsoft.azure.storage.core.ListResponse;
import com.microsoft.azure.storage.core.SR;
import com.microsoft.azure.storage.core.Utility;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Stack;

/**
 * RESERVED FOR INTERNAL USE. A class used to deserialize a list of shares.
 */
public class HandleListHandler extends DefaultHandler {

    private final Stack<String> elementStack = new Stack<String>();
    private StringBuilder bld = new StringBuilder();

    private final ListResponse<FileHandle> response = new ListResponse<FileHandle>();

    private FileHandle handle;

    private HandleListHandler() {
    }

    /**
     * Parse and return the response.
     *
     * @param stream
     * @return
     * @throws ParserConfigurationException
     * @throws SAXException
     * @throws IOException
     */
    protected static ListResponse<FileHandle> getHandleList(final InputStream stream)
            throws ParserConfigurationException, SAXException, IOException {
        SAXParser saxParser = Utility.getSAXParser();
        HandleListHandler handler = new HandleListHandler();
        saxParser.parse(stream, handler);

        return handler.response;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        this.elementStack.push(localName);

        if (FileConstants.HANDLE_ELEMENT.equals(localName)) {
            this.handle = new FileHandle();
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        String currentNode = this.elementStack.pop();

        // if the node popped from the stack and the localName don't match, the xml document is improperly formatted
        if (!localName.equals(currentNode)) {
            throw new SAXException(SR.INVALID_RESPONSE_RECEIVED);
        }

        String parentNode = null;
        if (!this.elementStack.isEmpty()) {
            parentNode = this.elementStack.peek();
        }

        String value = this.bld.toString();
        if (value.isEmpty()) {
            value = null;
        }

        if (FileConstants.HANDLE_ELEMENT.equals(currentNode)) {
            this.response.getResults().add(this.handle);
        }
        else if (ListResponse.ENUMERATION_RESULTS.equals(parentNode)) {
            if (Constants.NEXT_MARKER_ELEMENT.equals(currentNode)) {
                this.response.setNextMarker(value);
            }
        }
        else if (FileConstants.HANDLE_ELEMENT.equals(parentNode)) {
            try {
                this.setHandleProperties(currentNode, value);
            }
            catch (ParseException e) {
                throw new SAXException(e);
            }
        }

        this.bld = new StringBuilder();
    }

    @Override
    public void characters(char ch[], int start, int length) throws SAXException {
        this.bld.append(ch, start, length);
    }

    private void setHandleProperties(String currentNode, String value) throws ParseException {
        if (FileConstants.HANDLE_ID_ELEMENT.equals(currentNode)) {
            this.handle.setHandleID(value);
        }
        else if (FileConstants.PATH_ELEMENT.equals(currentNode)) {
            this.handle.setPath(value);
        }
        else if (FileConstants.FILE_ID_ELEMENT.equals(currentNode)) {
            this.handle.setFileID(value);
        }
        else if (FileConstants.PARENT_ID_ELEMENT.equals(currentNode)) {
            this.handle.setParentFileID(value);
        }
        else if (FileConstants.SESSION_ID_ELEMENT.equals(currentNode)) {
            this.handle.setSessionID(value);
        }
        else if (FileConstants.CLIENT_IP_ELEMENT.equals(currentNode)) {
            this.handle.setClientIP(value);
        }
        else if (FileConstants.OPEN_TIME_ELEMENT.equals(currentNode)) {
            this.handle.setOpenTime(value);
        }
        else if (FileConstants.LAST_RECONNECT_TIME_ELEMENT.equals(currentNode)) {
            this.handle.setLastReconnectTime(value);
        }
    }
}
