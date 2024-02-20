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

package cloud.xline.jxline.kv;

import cloud.xline.jxline.KeyValue;
import cloud.xline.jxline.impl.AbstractResponse;
import com.xline.protobuf.CommandResponse;
import com.xline.protobuf.SyncResponse;
import io.etcd.jetcd.ByteSequence;

public class PutResponse extends AbstractResponse<com.xline.protobuf.PutResponse> {

    private final ByteSequence namespace;

    public PutResponse(CommandResponse sr, SyncResponse asr, ByteSequence namespace) {
        super(sr, asr, CommandResponse::getPutResponse, s -> s.getPutResponse().getHeader());
        this.namespace = namespace;
    }

    public PutResponse(com.xline.protobuf.PutResponse putResponse, ByteSequence namespace) {
        super(putResponse, putResponse.getHeader());
        this.namespace = namespace;
    }

    /**
     * Returns previous key-value pair.
     *
     * @return prev kv.
     */
    public KeyValue getPrevKv() {
        return new KeyValue(getResponse().getPrevKv(), namespace);
    }

    /**
     * Returns whether a previous key-value pair is present.
     *
     * @return if has prev kv.
     */
    public boolean hasPrevKv() {
        return getResponse().hasPrevKv();
    }
}
