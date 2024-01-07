package cloud.xline.client;

import cloud.xline.client.exceptions.CommandExecutionException;
import com.curp.protobuf.*;
import com.curp.protobuf.ProtocolGrpc.ProtocolBlockingStub;
import com.google.protobuf.InvalidProtocolBufferException;
import com.xline.protobuf.Command;
import com.xline.protobuf.CommandResponse;
import com.xline.protobuf.ExecuteError;
import com.xline.protobuf.SyncResponse;
import io.grpc.*;
import org.apache.commons.math3.util.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

public class ProtocolClient {

    private static final Logger logger = Logger.getLogger(ProtocolClient.class.getName());
    private final State state;

    public ProtocolClient(ArrayList<ManagedChannel> channels) {
        ArrayList<ProtocolBlockingStub> tmpStubs = new ArrayList<>();
        for (ManagedChannel channel : channels) {
            ProtocolBlockingStub stub = ProtocolGrpc.newBlockingStub(channel);
            tmpStubs.add(stub);
        }
        FetchClusterResponse res = this.fetchCluster(tmpStubs);
        HashMap<Long, ProtocolBlockingStub> stubs = new HashMap<>();
        long clusterVersion = 0;
        for (Member member : res.getMembersList()) {
            ManagedChannel channel = Grpc.newChannelBuilder(member.getName(), InsecureChannelCredentials.create()).build();
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
     * @throws CommandExecutionException      on command execution error
     * @throws StatusRuntimeException         on server error
     */
    public CommandResponse propose(Command cmd, Boolean useFastPath) throws InvalidProtocolBufferException, CommandExecutionException {
        ProposeId id = this.getProposeId();
        // TODO
        //   Use non-blocking ProtocolClient(ProtocolFutureStub)
        //   instead of ProtocolBlockingStub to run fast round
        //   and slow round simultaneously
        try {
            return this.fastRound(id, cmd);
        } catch (Exception ignore) {
        }
        Pair<CommandResponse, SyncResponse> slowRes = this.slowRound(id);
        return slowRes.getFirst();
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
     * @throws StatusRuntimeException         on server error
     * @throws CommandExecutionException      on command execution error
     * @throws InvalidProtocolBufferException on deserialize error
     */
    public CommandResponse fastRound(ProposeId id, Command cmd) throws CommandExecutionException, InvalidProtocolBufferException {
        logger.info(String.format("Fast round start. Propose ID %s.", id));

        int okCnt = 0;
        CommandResponse exeRes = null;
        StatusRuntimeException err = null;

        Collection<ProtocolBlockingStub> stubs = this.state.getStubs().values();
        int superQuorum = this.superQuorum(stubs.size());

        for (ProtocolBlockingStub stub : stubs) {
            ProposeRequest propReq = ProposeRequest.newBuilder().setCommand(cmd.toByteString()).setProposeId(id).setClusterVersion(this.state.getClusterVersion()).build();
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
                logger.warning("Propose error: " + e);
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
     * @throws StatusRuntimeException         on server error
     * @throws InvalidProtocolBufferException on deserialize error
     * @throws CommandExecutionException      on command execution error
     */
    private Pair<CommandResponse, SyncResponse> slowRound(ProposeId id) throws InvalidProtocolBufferException, CommandExecutionException {
        logger.info(String.format("Slow round start. Propose ID %s.", id));
        long leaderId = this.state.getLeader();
        ProtocolBlockingStub leader = this.state.getStubs().get(leaderId);
        if (leader == null) {
            FetchClusterResponse res;
            do {
                res = this.fetchCluster(this.state.getStubs().values());
                this.state.checkUpdate(res);
            } while (res.hasLeaderId());
            leader = this.state.getStubs().get(res.getLeaderId());
            if (leader == null) {
                // mock a status error to outside if leader is still null
                throw new StatusRuntimeException(Status.DATA_LOSS);
            }
        }

        WaitSyncedRequest waitSyncReq = WaitSyncedRequest.newBuilder().setProposeId(id).setClusterVersion(this.state.getClusterVersion()).build();
        WaitSyncedResponse res = leader.waitSynced(waitSyncReq);
        if (res.getExeResult().hasError()) {
            throw new CommandExecutionException(ExecuteError.parseFrom(res.getExeResult().getError()));
        }
        if (res.getAfterSyncResult().hasError()) {
            throw new CommandExecutionException(ExecuteError.parseFrom(res.getAfterSyncResult().getError()));
        }
        CommandResponse er = CommandResponse.parseFrom(res.getExeResult().getOk());
        SyncResponse asr = SyncResponse.parseFrom(res.getAfterSyncResult().getOk());
        return Pair.create(er, asr);
    }

    /**
     * Fetch cluster from servers
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

    private class State {
        private final ReadWriteLock lock;
        private long leaderId;
        private long term;
        private long clusterVersion;
        private HashMap<Long, ProtocolBlockingStub> stubs;

        State(long leaderId, long term, long clusterVersion, HashMap<Long, ProtocolBlockingStub> stubs) {
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
            this.lock.writeLock().lock();
            if (res.getTerm() < this.term) {
                this.lock.writeLock().unlock();
                return;
            }
            if (res.hasLeaderId() && this.term < res.getTerm()) {
                this.term = res.getTerm();
                this.leaderId = res.getLeaderId();
                logger.config("client term updates to " + this.term);
                logger.config("client leader id updates to " + this.leaderId);
            }
            if (res.getClusterVersion() == this.clusterVersion) {
                this.lock.writeLock().unlock();
                return;
            }
            this.clusterVersion = res.getClusterVersion();
            HashMap<Long, ProtocolBlockingStub> stubs = new HashMap<>();
            for (Member member : res.getMembersList()) {
                // TODO: endpoint load balance?
                ManagedChannel channel = Grpc.newChannelBuilder(member.getAddrs(0), InsecureChannelCredentials.create()).build();
                ProtocolBlockingStub stub = ProtocolGrpc.newBlockingStub(channel);
                stubs.put(member.getId(), stub);
            }
            // TODO: do NOT drop the old stubs, instead modify the stubs (use ConcurrentHashMap)
            this.stubs = stubs;
            this.lock.writeLock().unlock();
        }
    }
}
