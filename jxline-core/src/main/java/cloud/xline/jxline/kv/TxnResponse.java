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

import static com.xline.protobuf.ResponseOp.ResponseCase.RESPONSE_DELETE_RANGE;
import static com.xline.protobuf.ResponseOp.ResponseCase.RESPONSE_PUT;
import static com.xline.protobuf.ResponseOp.ResponseCase.RESPONSE_RANGE;
import static com.xline.protobuf.ResponseOp.ResponseCase.RESPONSE_TXN;

import cloud.xline.jxline.impl.AbstractResponse;
import com.xline.protobuf.CommandResponse;
import com.xline.protobuf.SyncResponse;
import io.etcd.jetcd.ByteSequence;

import java.util.List;
import java.util.stream.Collectors;

/**
 * TxnResponse returned by a transaction call contains lists of put, get, delete responses
 * corresponding to either the compare in txn.IF is evaluated to true or false.
 */
public class TxnResponse extends AbstractResponse<com.xline.protobuf.TxnResponse> {

    private final ByteSequence namespace;

    private List<PutResponse> putResponses;
    private List<GetResponse> getResponses;
    private List<DeleteResponse> deleteResponses;
    private List<TxnResponse> txnResponses;

    /**
     * Creates a new TxnResponse based on the given {@link CommandResponse} and {@link
     * SyncResponse}.
     *
     * @param sr {@link CommandResponse}
     * @param asr {@link SyncResponse}
     * @param namespace the namespace of the response
     */
    public TxnResponse(CommandResponse sr, SyncResponse asr, ByteSequence namespace) {
        super(sr, asr, CommandResponse::getTxnResponse, s -> s.getTxnResponse().getHeader());
        this.namespace = namespace;
    }

    /**
     * Creates a new TxnResponse.
     *
     * @param txnResponse {@link com.xline.protobuf.TxnResponse}
     * @param namespace the namespace of the response
     */
    public TxnResponse(com.xline.protobuf.TxnResponse txnResponse, ByteSequence namespace) {
        super(txnResponse, txnResponse.getHeader());
        this.namespace = namespace;
    }

    /**
     * Returns true if the compare evaluated to true or false otherwise.
     *
     * @return if succeeded.
     */
    public boolean isSucceeded() {
        return getResponse().getSucceeded();
    }

    /**
     * Returns a list of DeleteResponse; empty list if none.
     *
     * @return delete responses.
     */
    public synchronized List<DeleteResponse> getDeleteResponses() {
        if (deleteResponses == null) {
            deleteResponses =
                    getResponse().getResponsesList().stream()
                            .filter(
                                    (responseOp) ->
                                            responseOp.getResponseCase() == RESPONSE_DELETE_RANGE)
                            .map(
                                    responseOp ->
                                            new DeleteResponse(
                                                    responseOp.getResponseDeleteRange(), namespace))
                            .collect(Collectors.toList());
        }

        return deleteResponses;
    }

    /**
     * Returns a list of GetResponse; empty list if none.
     *
     * @return get responses.
     */
    public synchronized List<GetResponse> getGetResponses() {
        if (getResponses == null) {
            getResponses =
                    getResponse().getResponsesList().stream()
                            .filter((responseOp) -> responseOp.getResponseCase() == RESPONSE_RANGE)
                            .map(
                                    responseOp ->
                                            new GetResponse(
                                                    responseOp.getResponseRange(), namespace))
                            .collect(Collectors.toList());
        }

        return getResponses;
    }

    /**
     * Returns a list of PutResponse; empty list if none.
     *
     * @return put responses.
     */
    public synchronized List<PutResponse> getPutResponses() {
        if (putResponses == null) {
            putResponses =
                    getResponse().getResponsesList().stream()
                            .filter((responseOp) -> responseOp.getResponseCase() == RESPONSE_PUT)
                            .map(
                                    responseOp ->
                                            new PutResponse(responseOp.getResponsePut(), namespace))
                            .collect(Collectors.toList());
        }

        return putResponses;
    }

    /**
     * Returns a list of TxnResponse; empty list if none.
     *
     * @return txn responses.
     */
    public synchronized List<TxnResponse> getTxnResponses() {
        if (txnResponses == null) {
            txnResponses =
                    getResponse().getResponsesList().stream()
                            .filter((responseOp) -> responseOp.getResponseCase() == RESPONSE_TXN)
                            .map(
                                    responseOp ->
                                            new TxnResponse(responseOp.getResponseTxn(), namespace))
                            .collect(Collectors.toList());
        }

        return txnResponses;
    }
}
