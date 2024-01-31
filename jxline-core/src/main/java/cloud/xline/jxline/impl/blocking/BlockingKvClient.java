package cloud.xline.jxline.impl.blocking;

import cloud.xline.jxline.exceptions.CommandExecutionException;

import com.google.protobuf.InvalidProtocolBufferException;
import com.xline.protobuf.*;

import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.List;

public class BlockingKvClient {
    private final BlockingProtocolClient curpClient;
    private final String token;

    public BlockingKvClient(BlockingProtocolClient curpClient, String token) {
        this.curpClient = curpClient;
        this.token = token;
    }

    /**
     * Get request
     *
     * @throws InvalidProtocolBufferException on deserialize error
     * @throws CommandExecutionException on command execution error
     * @throws StatusRuntimeException on server error
     */
    public RangeResponse get(RangeRequest req)
            throws InvalidProtocolBufferException, CommandExecutionException {
        KeyRange key =
                KeyRange.newBuilder().setKey(req.getKey()).setRangeEnd(req.getRangeEnd()).build();
        RequestWithToken req_token =
                RequestWithToken.newBuilder().setToken(this.token).setRangeRequest(req).build();
        Command cmd = Command.newBuilder().addKeys(0, key).setRequest(req_token).build();
        CommandResponse res = this.curpClient.propose(cmd, true);
        return res.getRangeResponse();
    }

    /**
     * Put request
     *
     * @throws InvalidProtocolBufferException on deserialize error
     * @throws CommandExecutionException on command execution error
     * @throws StatusRuntimeException on server error
     */
    public PutResponse put(PutRequest req)
            throws InvalidProtocolBufferException, CommandExecutionException {
        KeyRange key = KeyRange.newBuilder().setKey(req.getKey()).setRangeEnd(req.getKey()).build();
        RequestWithToken req_token =
                RequestWithToken.newBuilder().setToken(this.token).setPutRequest(req).build();
        Command cmd = Command.newBuilder().addKeys(0, key).setRequest(req_token).build();
        CommandResponse res = this.curpClient.propose(cmd, true);
        return res.getPutResponse();
    }

    /**
     * Delete request
     *
     * @throws InvalidProtocolBufferException on deserialize error
     * @throws CommandExecutionException on command execution error
     * @throws StatusRuntimeException on server error
     */
    public DeleteRangeResponse delete(DeleteRangeRequest req)
            throws InvalidProtocolBufferException, CommandExecutionException {
        KeyRange key =
                KeyRange.newBuilder().setKey(req.getKey()).setRangeEnd(req.getRangeEnd()).build();
        RequestWithToken req_token =
                RequestWithToken.newBuilder()
                        .setToken(this.token)
                        .setDeleteRangeRequest(req)
                        .build();
        Command cmd = Command.newBuilder().addKeys(0, key).setRequest(req_token).build();
        CommandResponse res = this.curpClient.propose(cmd, true);
        return res.getDeleteRangeResponse();
    }

    /**
     * Txn request
     *
     * @throws InvalidProtocolBufferException on deserialize error
     * @throws CommandExecutionException on command execution error
     * @throws StatusRuntimeException on server error
     */
    public TxnResponse txn(TxnRequest req)
            throws InvalidProtocolBufferException, CommandExecutionException {
        List<KeyRange> keyRanges = new ArrayList<>();
        for (Compare cmp : req.getCompareList()) {
            KeyRange key =
                    KeyRange.newBuilder().setKey(cmp.getKey()).setRangeEnd(cmp.getValue()).build();
            keyRanges.add(key);
        }
        RequestWithToken req_token =
                RequestWithToken.newBuilder().setToken(this.token).setTxnRequest(req).build();
        Command cmd = Command.newBuilder().addAllKeys(keyRanges).setRequest(req_token).build();
        CommandResponse res = this.curpClient.propose(cmd, true);
        return res.getTxnResponse();
    }

    /**
     * Compact request
     *
     * @throws InvalidProtocolBufferException on deserialize error
     * @throws CommandExecutionException on command execution error
     * @throws StatusRuntimeException on server error
     */
    public void compact(CompactionRequest req)
            throws InvalidProtocolBufferException, CommandExecutionException {
        boolean useFastPath = req.getPhysical();
        RequestWithToken req_token =
                RequestWithToken.newBuilder()
                        .setToken(this.token)
                        .setCompactionRequest(req)
                        .build();
        Command cmd = Command.newBuilder().setRequest(req_token).build();
        CommandResponse res = this.curpClient.propose(cmd, useFastPath);
        assert res != null;
    }
}
