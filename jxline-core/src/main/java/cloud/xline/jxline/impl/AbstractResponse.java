/*
 * Copyright 2016-2021 The jetcd authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cloud.xline.jxline.impl;

import com.xline.protobuf.CommandResponse;
import com.xline.protobuf.ResponseHeader;
import com.xline.protobuf.SyncResponse;
import io.etcd.jetcd.Response;

import java.util.function.Function;

/**
 * Build response based on some protobuf types
 *
 * @param <R> Protobuf types
 */
public class AbstractResponse<R> implements Response {

    private final R response;
    private final ResponseHeader responseHeader;
    private final Header header;

    /**
     * Create a new AbstractResponse based on the given response from curp server.
     *
     * @param sr CommandResponse
     * @param asr SyncResponse
     * @param mapping Mapping function
     * @param headerMapping Header mapping function
     */
    public AbstractResponse(
            CommandResponse sr,
            SyncResponse asr,
            Function<CommandResponse, R> mapping,
            Function<CommandResponse, ResponseHeader> headerMapping) {
        this.response = mapping.apply(sr);
        if (asr != null) {
            this.responseHeader =
                    headerMapping.apply(sr).toBuilder().setRevision(asr.getRevision()).build();
        } else {
            this.responseHeader = headerMapping.apply(sr);
        }
        this.header = new HeaderImpl();
    }

    /**
     * Create a new AbstractResponse.
     *
     * @param response The response
     * @param header The header
     */
    public AbstractResponse(R response, ResponseHeader header) {
        this.response = response;
        this.responseHeader = header;
        this.header = new HeaderImpl();
    }

    /**
     * Get the response header.
     *
     * @return the response header
     */
    @Override
    public Header getHeader() {
        return header;
    }

    @Override
    public String toString() {
        return response.toString();
    }

    /**
     * Get the response.
     *
     * @return the response
     */
    protected final R getResponse() {
        return this.response;
    }

    /**
     * Get the response header.
     *
     * @return the response header
     */
    protected final ResponseHeader getResponseHeader() {
        return this.responseHeader;
    }

    private class HeaderImpl implements Header {

        @Override
        public long getClusterId() {
            return responseHeader.getClusterId();
        }

        @Override
        public long getMemberId() {
            return responseHeader.getMemberId();
        }

        @Override
        public long getRevision() {
            return responseHeader.getRevision();
        }

        @Override
        public long getRaftTerm() {
            return responseHeader.getRaftTerm();
        }
    }
}
