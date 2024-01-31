package cloud.xline.jxline.impl.blocking;

import cloud.xline.jxline.exceptions.CommandExecutionException;

import com.curp.protobuf.*;
import com.curp.protobuf.ProtocolGrpc.ProtocolBlockingStub;
import com.google.protobuf.InvalidProtocolBufferException;
import com.xline.protobuf.Command;
import com.xline.protobuf.CommandResponse;
import com.xline.protobuf.ExecuteError;
import com.xline.protobuf.SyncResponse;

import io.grpc.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BlockingProtocolClient {
    private static final Logger logger = LoggerFactory.getLogger(BlockingProtocolClient.class);
    private final State state;

    public BlockingProtocolClient(ArrayList<ManagedChannel> channels) {
        ArrayList<ProtocolBlockingStub> tmpStubs = new ArrayList<>();
        for (ManagedChannel channel : channels) {
            ProtocolBlockingStub stub = ProtocolGrpc.newBlockingStub(channel);
            tmpStubs.add(stub);
        }
        FetchClusterResponse res = this.fetchCluster(tmpStubs);
        HashMap<Long, ProtocolBlockingStub> stubs = new HashMap<>();
        long clusterVersion = 0;
        for (Member member : res.getMembersList()) {
            // TODO: endpoint load balance
            ManagedChannel channel =
                    Grpc.newChannelBuilder(member.getAddrs(0), InsecureChannelCredentials.create())
                            .build();
            ProtocolBlockingStub stub = ProtocolGrpc.newBlockingStub(channel);
            stubs.put(member.getId(), stub);
            clusterVersion = res.getClusterVersion();
        }
        this.state = new State(res.getLeaderId(), res.getTerm(), clusterVersion, stubs);
    }

    /**
     * Propose command
     *
     * @return CommandResponse if success
     * @throws InvalidProtocolBufferException on deserialize error
     * @throws CommandExecutionException on command execution error
     * @throws StatusRuntimeException on server error
     */
    public CommandResponse propose(Command cmd, Boolean useFastPath)
            throws InvalidProtocolBufferException, CommandExecutionException {
        ProposeId id = this.getProposeId();
        // TODO
        //   Use non-blocking ProtocolClient(ProtocolFutureStub)
        //   instead of ProtocolBlockingStub to run fast round
        //   and slow round simultaneously
        try {
            return this.fastRound(id, cmd);
        } catch (Exception ignore) {
        }
        Map.Entry<CommandResponse, SyncResponse> slowRes = this.slowRound(id);
        return slowRes.getKey();
    }

    private ProposeId getProposeId() {
        UUID uuid = UUID.randomUUID();
        // TODO: obtain from server
        long clientId = uuid.getMostSignificantBits();
        // TODO: implement tracker
        long seqNum = uuid.getMostSignificantBits();
        return ProposeId.newBuilder().setClientId(clientId).setSeqNum(seqNum).build();
    }

    /**
     * Run fast round
     *
     * @return return the CommandResponse
     * @throws StatusRuntimeException on server error
     * @throws CommandExecutionException on command execution error
     * @throws InvalidProtocolBufferException on deserialize error
     */
    public CommandResponse fastRound(ProposeId id, Command cmd)
            throws CommandExecutionException, InvalidProtocolBufferException {
        logger.info(String.format("Fast round start. Propose ID %s.", id));

        int okCnt = 0;
        CommandResponse exeRes = null;
        StatusRuntimeException err = null;

        Collection<ProtocolBlockingStub> stubs = this.state.getStubs().values();
        int superQuorum = this.superQuorum(stubs.size());

        for (ProtocolBlockingStub stub : stubs) {
            ProposeRequest propReq =
                    ProposeRequest.newBuilder()
                            .setCommand(cmd.toByteString())
                            .setProposeId(id)
                            .setClusterVersion(this.state.getClusterVersion())
                            .build();
            try {
                ProposeResponse res = stub.propose(propReq);
                okCnt++;
                if (res.hasResult()) {
                    CmdResult cmdResult = res.getResult();
                    if (cmdResult.hasOk()) {
                        if (exeRes != null) {
                            throw new RuntimeException("Should not set exe result twice.");
                        }
                        try {
                            exeRes = CommandResponse.parseFrom(cmdResult.getOk());
                        } catch (Exception e) {
                            throw new RuntimeException("Deserialize error", e);
                        }
                    }
                    if (cmdResult.hasError()) {
                        ExecuteError exeErr = ExecuteError.parseFrom(cmdResult.getError());
                        throw new CommandExecutionException(exeErr);
                    }
                }
                if (exeRes != null && okCnt >= superQuorum) {
                    return exeRes;
                }
            } catch (StatusRuntimeException e) {
                logger.warn("Propose error: " + e);
                err = e;
            }
        }
        if (err != null) {
            throw err;
        }
        throw new RuntimeException("Leader should return ER if no error happens.");
    }

    /**
     * Run slow round
     *
     * @return return the Pair<CommandResponse, SyncResponse>
     * @throws StatusRuntimeException on server error
     * @throws InvalidProtocolBufferException on deserialize error
     * @throws CommandExecutionException on command execution error
     */
    private Map.Entry<CommandResponse, SyncResponse> slowRound(ProposeId id)
            throws InvalidProtocolBufferException, CommandExecutionException {
        logger.info(String.format("Slow round start. Propose ID %s.", id));
        ProtocolBlockingStub leader = this.mustGetLeaderStub();
        WaitSyncedRequest waitSyncReq =
                WaitSyncedRequest.newBuilder()
                        .setProposeId(id)
                        .setClusterVersion(this.state.getClusterVersion())
                        .build();
        WaitSyncedResponse res = leader.waitSynced(waitSyncReq);
        if (res.getExeResult().hasError()) {
            throw new CommandExecutionException(
                    ExecuteError.parseFrom(res.getExeResult().getError()));
        }
        if (res.getAfterSyncResult().hasError()) {
            throw new CommandExecutionException(
                    ExecuteError.parseFrom(res.getAfterSyncResult().getError()));
        }
        CommandResponse er = CommandResponse.parseFrom(res.getExeResult().getOk());
        SyncResponse asr = SyncResponse.parseFrom(res.getAfterSyncResult().getOk());
        return Map.entry(er, asr);
    }

    /**
     * Fetch cluster from servers TODO: Needs to be refactored when cluster server refactored
     *
     * @throws StatusRuntimeException on server error (servers all failed)
     */
    private FetchClusterResponse fetchCluster(Collection<ProtocolBlockingStub> stubs) {
        StatusRuntimeException err = null;
        for (ProtocolBlockingStub stub : stubs) {
            try {
                return stub.fetchCluster(FetchClusterRequest.newBuilder().build());
            } catch (StatusRuntimeException e) {
                err = e;
            }
        }
        if (err != null) {
            throw err;
        }
        throw new RuntimeException("Empty stubs.");
    }

    private int superQuorum(int nodes) {
        int faultTolerance = nodes / 2;
        int quorum = faultTolerance + 1;
        return faultTolerance + (quorum / 2) + 1;
    }

    /**
     * Must get the leader stub
     *
     * @return leader stub
     */
    ProtocolBlockingStub mustGetLeaderStub() {
        try {
            this.state.lock.readLock().lock();
            ProtocolBlockingStub leader = this.state.stubs.get(this.state.leaderId);
            if (leader == null) {
                FetchClusterResponse res;
                do {
                    res = this.fetchCluster(this.state.stubs.values());
                    this.state.checkUpdate(res);
                } while (res.hasLeaderId());
                leader = this.state.stubs.get(res.getLeaderId());
                if (leader == null) {
                    // mock a status error to outside if leader is still null
                    throw new StatusRuntimeException(Status.DATA_LOSS);
                }
            }
            return leader;
        } finally {
            this.state.lock.readLock().unlock();
        }
    }

    private static class State {
        private final ReadWriteLock lock;
        private long leaderId;
        private long term;
        private long clusterVersion;
        private HashMap<Long, ProtocolBlockingStub> stubs;

        State(
                long leaderId,
                long term,
                long clusterVersion,
                HashMap<Long, ProtocolBlockingStub> stubs) {
            this.lock = new ReentrantReadWriteLock();
            this.leaderId = leaderId;
            this.term = term;
            this.clusterVersion = clusterVersion;
            this.stubs = stubs;
        }

        long getClusterVersion() {
            this.lock.readLock().lock();
            long version = this.clusterVersion;
            this.lock.readLock().unlock();
            return version;
        }

        HashMap<Long, ProtocolBlockingStub> getStubs() {
            this.lock.readLock().lock();
            HashMap<Long, ProtocolBlockingStub> stubs = this.stubs;
            this.lock.readLock().unlock();
            return stubs;
        }

        long getLeader() {
            this.lock.readLock().lock();
            long res = this.leaderId;
            this.lock.readLock().unlock();
            return res;
        }

        void checkUpdate(FetchClusterResponse res) {
            try {
                this.lock.writeLock().lock();
                if (res.getTerm() < this.term) {
                    return;
                }
                if (res.hasLeaderId() && this.term < res.getTerm()) {
                    this.term = res.getTerm();
                    this.leaderId = res.getLeaderId();
                    logger.info("client term updates to " + this.term);
                    logger.info("client leader id updates to " + this.leaderId);
                }
                if (res.getClusterVersion() == this.clusterVersion) {
                    return;
                }
                this.clusterVersion = res.getClusterVersion();
                HashMap<Long, ProtocolBlockingStub> stubs = new HashMap<>();
                for (Member member : res.getMembersList()) {
                    // TODO: endpoint load balance
                    ManagedChannel channel =
                            Grpc.newChannelBuilder(
                                            member.getAddrs(0), InsecureChannelCredentials.create())
                                    .build();
                    ProtocolBlockingStub stub = ProtocolGrpc.newBlockingStub(channel);
                    stubs.put(member.getId(), stub);
                }
                // TODO: do NOT drop the old stubs, instead modify the stubs (use ConcurrentHashMap)
                this.stubs = stubs;
            } finally {
                this.lock.writeLock().unlock();
            }
        }
    }
}
