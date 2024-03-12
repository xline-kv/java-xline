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
import com.xline.protobuf.DeleteRangeResponse;
import com.xline.protobuf.SyncResponse;
import io.etcd.jetcd.ByteSequence;

import java.util.List;
import java.util.stream.Collectors;

public class DeleteResponse extends AbstractResponse<DeleteRangeResponse> {

    private final ByteSequence namespace;

    private List<KeyValue> prevKvs;

    public DeleteResponse(DeleteRangeResponse deleteRangeResponse, ByteSequence namespace) {
        super(deleteRangeResponse, deleteRangeResponse.getHeader());
        this.namespace = namespace;
    }

    public DeleteResponse(CommandResponse sr, SyncResponse asr, ByteSequence namespace) {
        super(sr, asr, CommandResponse::getDeleteRangeResponse, DeleteRangeResponse::getHeader);
        this.namespace = namespace;
    }

    /**
     * Returns the number of keys deleted by the delete range request.
     *
     * @return number of deleted items.
     */
    public long getDeleted() {
        return getResponse().getDeleted();
    }

    /**
     * Returns previous key-value pairs.
     *
     * @return previous kv,
     */
    public synchronized List<KeyValue> getPrevKvs() {
        if (prevKvs == null) {
            prevKvs =
                    getResponse().getPrevKvsList().stream()
                            .map(kv -> new KeyValue(kv, namespace))
                            .collect(Collectors.toList());
        }

        return prevKvs;
    }
}
