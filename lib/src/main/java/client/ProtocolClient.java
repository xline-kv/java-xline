package client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.math3.util.Pair;
import com.curp.protobuf.CmdResult;
import com.curp.protobuf.CommandSyncError;
import com.curp.protobuf.FetchClusterRequest;
import com.curp.protobuf.FetchClusterResponse;
import com.curp.protobuf.Member;
import com.curp.protobuf.ProposeError;
import com.curp.protobuf.ProposeRequest;
import com.curp.protobuf.ProposeResponse;
import com.curp.protobuf.ProtocolGrpc;
import com.curp.protobuf.WaitSyncedRequest;
import com.curp.protobuf.WaitSyncedResponse;
import com.curp.protobuf.ProtocolGrpc.ProtocolBlockingStub;
import com.curp.protobuf.WaitSyncedResponse.Success;
import com.xline.protobuf.Command;
import com.xline.protobuf.CommandResponse;
import com.xline.protobuf.ExecuteError;
import com.xline.protobuf.SyncResponse;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;

public class ProtocolClient {
    private String localServerId;
    private HashMap<String, ProtocolBlockingStub> stubs;
    private List<Member> members;
    private static final Logger logger = Logger.getLogger(ProtocolClient.class.getName());

    public ProtocolClient(ArrayList<ManagedChannel> channels) {
        ArrayList<ProtocolBlockingStub> tempStubs = new ArrayList<ProtocolBlockingStub>();

        for (ManagedChannel channel : channels) {
            ProtocolBlockingStub tempStub = ProtocolGrpc.newBlockingStub(channel);
            tempStubs.add(tempStub);
        }

        FetchClusterResponse res = fetchCluster(tempStubs);

        HashMap<String, ProtocolBlockingStub> stubs = new HashMap<String, ProtocolBlockingStub>();
        List<Member> members = res.getMembersList();
        for (Member member : members) {
            ManagedChannel channel = Grpc.newChannelBuilder(member.getName(), InsecureChannelCredentials.create())
                    .build();
            ProtocolBlockingStub stub = ProtocolGrpc.newBlockingStub(channel);
            stubs.put(Long.toString(member.getId()), stub);
        }

        this.localServerId = Long.toString(res.getLeaderId());
        this.stubs = stubs;
        this.members = members;
    }

    private FetchClusterResponse fetchCluster(ArrayList<ProtocolBlockingStub> blockingStubs) {
        for (ProtocolBlockingStub blockingStub : blockingStubs) {
            try {
                return blockingStub.fetchCluster(FetchClusterRequest.newBuilder().build());
            } catch (Exception e) {
                logger.info(e.getMessage());
            }
        }
        throw new RuntimeException("Fetch cluster fail.");
    }

    public CommandResponse propose(Command cmd, Boolean useFastPath) {
        Pair<CommandResponse, Boolean> fastRes = this.fastRound(cmd);
        Pair<SyncResponse, CommandResponse> slowRes = this.slowRound(cmd);
        if (useFastPath) {
            if (fastRes.getSecond()) {
                return fastRes.getFirst();
            } else {
                return slowRes.getSecond();
            }
        } else {
            return slowRes.getSecond();
        }
    }

    public Pair<CommandResponse, Boolean> fastRound(Command cmd) {
        logger.info(String.format("Fast round start. Propose ID %s.", cmd.getProposeId()));

        int okCnt = 0;
        CommandResponse exeRes = null;

        for (ProtocolBlockingStub stub : this.stubs.values()) {
            ProposeRequest propReq = ProposeRequest.newBuilder().setCommand(cmd.toByteString()).build();
            ProposeResponse res = stub.propose(propReq);

            if (res.hasResult()) {
                CmdResult cmdResult = res.getResult();
                okCnt++;
                if (cmdResult.hasEr()) {
                    if (exeRes != null) {
                        throw new RuntimeException("Should not set exe result twice.");
                    }
                    try {
                        exeRes = CommandResponse.parseFrom(cmdResult.getEr());
                    } catch (Exception e) {
                        throw new RuntimeException("Should have exe result.", e);
                    }
                }
                if (cmdResult.hasError()) {
                    try {
                        ExecuteError exeErr = ExecuteError.parseFrom(cmdResult.getError());
                        throw new RuntimeException(exeErr.toString());
                    } catch (Exception e) {
                        throw new RuntimeException("Should have exe error.", e);
                    }
                }
            } else if (res.hasError()) {
                ProposeError propErr = res.getError();
                logger.warning(propErr.toString());
            } else {
                okCnt++;
            }

            if (exeRes != null && okCnt >= this.superQuorum(this.stubs.size())) {
                return Pair.create(exeRes, true);
            }
        }
        return Pair.create(exeRes, false);
    }

    private Pair<SyncResponse, CommandResponse> slowRound(Command cmd) {
        logger.info(String.format("Slow round start. Propose ID %s.", cmd.getProposeId()));

        SyncResponse syncRes = null;
        CommandResponse cmdRes = null;

        for (Member member : this.members) {
            if (Long.toString(member.getId()).equals(this.localServerId)) {
                ManagedChannel channel = Grpc.newChannelBuilder(member.getName(), InsecureChannelCredentials.create())
                        .build();
                ProtocolBlockingStub stub = ProtocolGrpc.newBlockingStub(channel);

                WaitSyncedRequest waitSyncReq = WaitSyncedRequest.newBuilder().setProposeId(cmd.getProposeId()).build();
                WaitSyncedResponse res = stub.waitSynced(waitSyncReq);

                if (res.hasSuccess()) {
                    Success success = res.getSuccess();
                    try {
                        syncRes = SyncResponse.parseFrom(success.getAfterSyncResult());
                    } catch (Exception e) {
                        throw new RuntimeException("Should have sync response.", e);
                    }
                    try {
                        cmdRes = CommandResponse.parseFrom(success.getExeResult());
                    } catch (Exception e) {
                        throw new RuntimeException("Should have command response.", e);
                    }
                    return Pair.create(syncRes, cmdRes);
                } else if (res.hasError()) {
                    CommandSyncError err = res.getError();
                    if (err.hasWaitSync()) {
                        logger.warning(err.getWaitSync().toString());
                    } else if (err.hasExecute() || err.hasAfterSync()) {
                        try {
                            ExecuteError exeErr = ExecuteError.parseFrom(err.getExecute());
                            logger.warning(exeErr.toString());
                        } catch (Exception e) {
                            throw new RuntimeException("Unknown command execute error.");
                        }
                    } else if (err.hasShutdown()) {
                        logger.warning(err.getShutdown().toString());
                    } else {
                        throw new RuntimeException("Unknown command sync error.");
                    }
                } else {
                    throw new RuntimeException("Unknown wait sync response.");
                }
            }
        }
        return Pair.create(syncRes, cmdRes);
    }

    private int superQuorum(int nodes) {
        int faultTolerance = nodes / 2;
        int quorum = faultTolerance + 1;
        int superQuorum = faultTolerance + (quorum / 2) + 1;
        return superQuorum;
    }
}
