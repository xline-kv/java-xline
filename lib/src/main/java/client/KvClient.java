package client;

import java.util.UUID;
import com.xline.protobuf.Command;
import com.xline.protobuf.CommandResponse;
import com.xline.protobuf.CompactionRequest;
import com.xline.protobuf.CompactionResponse;
import com.xline.protobuf.Compare;
import com.xline.protobuf.DeleteRangeRequest;
import com.xline.protobuf.DeleteRangeResponse;
import com.xline.protobuf.KeyRange;
import com.xline.protobuf.PutRequest;
import com.xline.protobuf.PutResponse;
import com.xline.protobuf.RangeRequest;
import com.xline.protobuf.RangeResponse;
import com.xline.protobuf.RequestWithToken;
import com.xline.protobuf.TxnRequest;
import com.xline.protobuf.TxnResponse;

import java.util.List;
import java.util.ArrayList;

public class KvClient {
    private String name;
    private ProtocolClient curpClient;
    private String token;

    public KvClient(String name, ProtocolClient curpClient, String token) {
        this.name = name;
        this.curpClient = curpClient;
        this.token = token;
    }

    public RangeResponse get(RangeRequest req) {
        KeyRange key = KeyRange.newBuilder().setKey(req.getKey()).setRangeEnd(req.getRangeEnd()).build();
        RequestWithToken req_token = RequestWithToken.newBuilder().setToken(this.token).setRangeRequest(req).build();
        String proposeId = this.generateProposeId();
        Command cmd = Command.newBuilder().addKeys(0, key).setRequest(req_token).setProposeId(proposeId).build();
        CommandResponse res = this.curpClient.propose(cmd, true);
        return res.getRangeResponse();
    }

    public PutResponse put(PutRequest req) {
        KeyRange key = KeyRange.newBuilder().setKey(req.getKey()).setRangeEnd(req.getKey()).build();
        RequestWithToken req_token = RequestWithToken.newBuilder().setToken(this.token).setPutRequest(req).build();
        String proposeId = this.generateProposeId();
        Command cmd = Command.newBuilder().addKeys(0, key).setRequest(req_token).setProposeId(proposeId).build();
        CommandResponse res = this.curpClient.propose(cmd, false);
        return res.getPutResponse();
    }

    public DeleteRangeResponse delete(DeleteRangeRequest req) {
        KeyRange key = KeyRange.newBuilder().setKey(req.getKey()).setRangeEnd(req.getRangeEnd()).build();
        RequestWithToken req_token = RequestWithToken.newBuilder().setToken(this.token).setDeleteRangeRequest(req)
                .build();
        String proposeId = this.generateProposeId();
        Command cmd = Command.newBuilder().addKeys(0, key).setRequest(req_token).setProposeId(proposeId).build();
        CommandResponse res = this.curpClient.propose(cmd, false);
        return res.getDeleteRangeResponse();
    }

    public TxnResponse txn(TxnRequest req) {
        List<KeyRange> keyRanges = new ArrayList<KeyRange>();
        for (Compare cmp : req.getCompareList()) {
            KeyRange key = KeyRange.newBuilder().setKey(cmp.getKey()).setRangeEnd(cmp.getValue()).build();
            keyRanges.add(key);
        }
        RequestWithToken req_token = RequestWithToken.newBuilder().setToken(this.token).setTxnRequest(req).build();
        String proposeId = this.generateProposeId();
        Command cmd = Command.newBuilder().addAllKeys(keyRanges).setRequest(req_token).setProposeId(proposeId).build();
        CommandResponse res = this.curpClient.propose(cmd, false);
        return res.getTxnResponse();
    }

    public CompactionResponse compact(CompactionRequest req) {
        boolean useFastPath = req.getPhysical();
        RequestWithToken req_token = RequestWithToken.newBuilder().setToken(this.token).setCompactionRequest(req)
                .build();
        String proposeId = this.generateProposeId();
        Command cmd = Command.newBuilder().setRequest(req_token).setProposeId(proposeId).build();
        CommandResponse res = this.curpClient.propose(cmd, useFastPath);
        return res.getCompactionResponse();
    }

    private String generateProposeId() {
        String proposeId = this.name + "-" + UUID.randomUUID().toString();
        return proposeId;
    }
}
