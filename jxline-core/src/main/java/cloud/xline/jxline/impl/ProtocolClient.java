package cloud.xline.jxline.impl;

import cloud.xline.jxline.utils.Pair;
import com.curp.protobuf.FetchClusterResponse;
import com.curp.protobuf.ProposeId;
import com.curp.protobuf.VertxProtocolGrpc;
import com.xline.protobuf.Command;
import com.xline.protobuf.CommandResponse;
import com.xline.protobuf.SyncResponse;

import java.util.concurrent.CompletableFuture;

class ProtocolClient extends Impl {
    private final VertxProtocolGrpc.ProtocolVertxStub stub;

    protected ProtocolClient(ClientConnectionManager connectionManager) {
        super(connectionManager);
        this.stub = connectionManager.newStub(VertxProtocolGrpc::newVertxStub);
    }

    CompletableFuture<CommandResponse> fastRound(ProposeId id, Command cmd) {
        throw new RuntimeException("unimplemented");
    }

    CompletableFuture<Pair<CommandResponse, SyncResponse>> slowRound(ProposeId id) {
        throw new RuntimeException("unimplemented");
    }

    CompletableFuture<FetchClusterResponse> fetchCluster(boolean linearizable) {
        throw new RuntimeException("unimplemented");
    }
}
