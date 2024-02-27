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

public class AbstractResponse<R> implements Response {

    private final R response;
    private final ResponseHeader responseHeader;
    private final Header header;

    public AbstractResponse(
            CommandResponse sr,
            SyncResponse asr,
            Function<CommandResponse, R> mapping,
            Function<R, ResponseHeader> headerMapping) {
        this.response = mapping.apply(sr);
        if (asr != null) {
            this.responseHeader =
                    headerMapping.apply(this.response).toBuilder()
                            .setRevision(asr.getRevision())
                            .build();
        } else {
            this.responseHeader = headerMapping.apply(this.response);
        }
        this.header = new HeaderImpl();
    }

    public AbstractResponse(R response, ResponseHeader header) {
        this.response = response;
        this.responseHeader = header;
        this.header = new HeaderImpl();
    }

    @Override
    public Header getHeader() {
        return header;
    }

    @Override
    public String toString() {
        return response.toString();
    }

    protected final R getResponse() {
        return this.response;
    }

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
