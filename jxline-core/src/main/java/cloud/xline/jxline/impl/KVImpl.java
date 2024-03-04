package cloud.xline.jxline.impl;

import cloud.xline.jxline.KV;
import cloud.xline.jxline.ProtocolClient;
import cloud.xline.jxline.Txn;
import cloud.xline.jxline.kv.*;
import cloud.xline.jxline.support.Requests;
import com.xline.protobuf.Command;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.options.CompactOption;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;

import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

class KVImpl extends Impl implements KV {

    private final ProtocolClient protocolClient;

    public KVImpl(ProtocolClient protocolClient, ClientConnectionManager manager) {
        super(manager);
        this.protocolClient = protocolClient;
    }

    @Override
    public CompletableFuture<PutResponse> put(ByteSequence key, ByteSequence value) {
        return this.put(key, value, PutOption.DEFAULT);
    }

    @Override
    public CompletableFuture<PutResponse> put(
            ByteSequence key, ByteSequence value, PutOption option) {
        requireNonNull(key, "key should not be null");
        requireNonNull(value, "value should not be null");
        requireNonNull(option, "option should not be null");
        Command cmd =
                Requests.mapPutCommand(key, value, option, this.connectionManager().getNamespace());
        return protocolClient.propose(
                cmd,
                true,
                (sr, asr) -> new PutResponse(sr, asr, this.connectionManager().getNamespace()));
    }

    @Override
    public CompletableFuture<GetResponse> get(ByteSequence key) {
        requireNonNull(key, "key should not be null");
        return get(key, GetOption.DEFAULT);
    }

    @Override
    public CompletableFuture<GetResponse> get(ByteSequence key, GetOption option) {
        requireNonNull(key, "key should not be null");
        requireNonNull(option, "option should not be null");
        Command cmd =
                Requests.mapRangeCommand(key, option, this.connectionManager().getNamespace());
        return protocolClient.propose(
                cmd,
                true,
                (sr, asr) -> new GetResponse(sr, asr, this.connectionManager().getNamespace()));
    }

    @Override
    public CompletableFuture<DeleteResponse> delete(ByteSequence key) {
        requireNonNull(key, "key should not be null");
        return delete(key, DeleteOption.DEFAULT);
    }

    @Override
    public CompletableFuture<DeleteResponse> delete(ByteSequence key, DeleteOption option) {
        requireNonNull(key, "key should not be null");
        requireNonNull(option, "option should not be null");
        Command cmd =
                Requests.mapDeleteCommand(key, option, this.connectionManager().getNamespace());
        return protocolClient.propose(
                cmd,
                true,
                (sr, asr) -> new DeleteResponse(sr, asr, this.connectionManager().getNamespace()));
    }

    @Override
    public CompletableFuture<CompactResponse> compact(long revision) {
        return compact(revision, CompactOption.DEFAULT);
    }

    @Override
    public CompletableFuture<CompactResponse> compact(long revision, CompactOption option) {
        requireNonNull(option, "option should not be null");
        Command cmd = Requests.mapCompactRequest(revision, option);
        return protocolClient.propose(cmd, true, CompactResponse::new);
    }

    @Override
    public Txn txn() {
        return null;
    }
}
