package cloud.xline.jxline.impl;

import cloud.xline.jxline.exceptions.CurpException;
import cloud.xline.jxline.exceptions.XlineException;
import cloud.xline.jxline.utils.Invoke;
import cloud.xline.jxline.utils.Pair;
import com.curp.protobuf.*;
import com.google.protobuf.Empty;
import com.google.protobuf.InvalidProtocolBufferException;
import com.xline.protobuf.Command;
import com.xline.protobuf.CommandResponse;
import com.xline.protobuf.ExecuteError;
import com.xline.protobuf.SyncResponse;
import io.etcd.jetcd.resolver.IPNameResolver;
import io.grpc.ManagedChannel;
import io.vertx.core.Future;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

class ProtocolClient extends Impl {

    private final State state;

    private ProposeId getProposeId() {
        UUID uuid = UUID.randomUUID();
        // TODO: obtain from server
        long clientId = uuid.getMostSignificantBits();
        // TODO: implement tracker
        long seqNum = uuid.getMostSignificantBits();
        return ProposeId.newBuilder().setClientId(clientId).setSeqNum(seqNum).build();
    }

    <T> CompletableFuture<T> propose(
            Command cmd,
            boolean useFastPath,
            Function<Pair<CommandResponse, SyncResponse>, T> convert) {
        ProposeId id = this.getProposeId();
        Executor executor = connectionManager().getExecutorService();
        if (!useFastPath) {
            return CompletableFuture.supplyAsync(() -> this.fastRound(id, cmd), executor)
                    .handleAsync((r, ex) -> this.slowRound(id), executor)
                    .thenApply(convert);
        }
        CompletionService<T> service =
                new ExecutorCompletionService<>(connectionManager().getExecutorService());
        service.submit(() -> convert.apply(new Pair<>(this.fastRound(id, cmd), null)));
        service.submit(() -> convert.apply(this.slowRound(id)));

        return CompletableFuture.supplyAsync(
                () -> {
                    CurpException exception = null;
                    for (int i = 0; true; i++) {
                        try {
                            return service.take().get();
                        } catch (InterruptedException e) {
                            throw XlineException.toXlineException(e);
                        } catch (ExecutionException e) {
                            Throwable cause = e.getCause();
                            if (cause instanceof XlineException) {
                                throw (XlineException) cause;
                            }
                            if (cause instanceof CurpException) {
                                CurpException ex = ((CurpException) cause);
                                if (ex.shouldAbortSlowRound()) {
                                    throw (CurpException) cause;
                                }
                                if (exception == null
                                        || exception.priority().value() <= ex.priority().value()) {
                                    exception = ex;
                                }
                                if (i == 1) {
                                    throw exception;
                                }
                                continue;
                            }
                            throw XlineException.toXlineException(e);
                        }
                    }
                },
                executor);
    }

    Pair<CommandResponse, SyncResponse> slowRound(ProposeId id) {
        logger().info(String.format("Slow round start. Propose ID %s.", id));
        WaitSyncedRequest waitSyncReq =
                WaitSyncedRequest.newBuilder()
                        .setProposeId(id)
                        .setClusterVersion(this.state.getClusterVersion())
                        .build();
        try {
            WaitSyncedResponse resp =
                    completable(
                                    mapLeader(stub -> stub.waitSynced(waitSyncReq)),
                                    res -> res,
                                    CurpException::toCurpException)
                            .get();
            if (resp.getExeResult().hasError()) {
                throw new XlineException(ExecuteError.parseFrom(resp.getExeResult().getError()));
            }
            if (resp.getAfterSyncResult().hasError()) {
                throw new XlineException(
                        ExecuteError.parseFrom(resp.getAfterSyncResult().getError()));
            }
            CommandResponse er = CommandResponse.parseFrom(resp.getExeResult().getOk());
            SyncResponse asr = SyncResponse.parseFrom(resp.getAfterSyncResult().getOk());
            return new Pair<>(er, asr);
        } catch (InterruptedException | InvalidProtocolBufferException e) {
            throw XlineException.toXlineException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (!(cause instanceof CurpException)) {
                throw XlineException.toXlineException(cause);
            }
            throw (CurpException) cause;
        }
    }

    /**
     * Run fastRound
     *
     * @param id The Proposal id
     * @param cmd The command
     * @return {@link CommandResponse}
     * @throws {@link XlineException} when got serializing error, command execution error or
     *     unexpected behavior, {@link CurpException} when got curp error
     */
    CommandResponse fastRound(ProposeId id, Command cmd) {
        logger().info(String.format("Fast round start. Propose ID %s.", id));
        ProposeRequest propReq =
                ProposeRequest.newBuilder()
                        .setCommand(cmd.toByteString())
                        .setProposeId(id)
                        .setClusterVersion(this.state.getClusterVersion())
                        .build();

        Collection<VertxProtocolGrpc.ProtocolVertxStub> stubs = this.state.getStubs().values();
        int okCnt = 0;
        int superQuorum = this.superQuorum(stubs.size());
        CommandResponse exeRes = null;
        CurpException exception = null;

        CompletionService<CommandResponse> completionService =
                forEachServer(
                        stubs,
                        stub -> {
                            CmdResult cmdResult =
                                    completable(
                                                    stub.propose(propReq),
                                                    ProposeResponse::getResult,
                                                    CurpException::toCurpException)
                                            .get();
                            if (cmdResult.hasError()) {
                                ExecuteError error = ExecuteError.parseFrom(cmdResult.getError());
                                throw new XlineException(error);
                            }
                            if (cmdResult.hasOk()) {
                                return CommandResponse.parseFrom(cmdResult.getOk());
                            }
                            return null;
                        });

        for (int i = 0; i < stubs.size(); i++) {
            try {
                CommandResponse resp = completionService.take().get();
                if (resp != null) {
                    exeRes = resp;
                }
                okCnt++;
                if (okCnt >= superQuorum && exeRes != null) {
                    return exeRes;
                }
            } catch (InterruptedException e) {
                // unexpected exception
                throw XlineException.toXlineException(e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (!(cause instanceof CurpException)) {
                    throw XlineException.toXlineException(cause);
                }
                CurpException ex = (CurpException) cause;
                if (ex.shouldAbortFastRound()) {
                    throw ex;
                }
                if (exception == null || exception.priority().value() <= ex.priority().value()) {
                    exception = ex;
                }
            }
        }
        if (exception != null) {
            throw exception;
        }
        // We will at least send the request to the leader if no `WrongClusterVersion` returned.
        // If no errors occur, the leader should return the ER
        // If it is because the super quorum has not been reached, an error will definitely occur.
        // Otherwise, there is no leader in the cluster state currently, return wrong cluster
        // version
        // and attempt to retrieve the cluster state again.
        throw new CurpException(
                CurpError.newBuilder().setWrongClusterVersion(Empty.newBuilder().build()).build());
    }

    <T> CompletionService<T> forEachServer(
            Collection<VertxProtocolGrpc.ProtocolVertxStub> stubs,
            Invoke<VertxProtocolGrpc.ProtocolVertxStub, T> task) {
        CompletionService<T> completionService =
                new ExecutorCompletionService<>(this.connectionManager().getExecutorService());
        for (VertxProtocolGrpc.ProtocolVertxStub stub : stubs) {
            completionService.submit(() -> task.call(stub));
        }
        return completionService;
    }

    <T> Future<T> mapLeader(Function<VertxProtocolGrpc.ProtocolVertxStub, Future<T>> task) {
        VertxProtocolGrpc.ProtocolVertxStub leaderStub = this.state.getLeaderStub();
        if (leaderStub == null) {
            // choose a random leader, it will return redirect error if leader is wrong
            // TODO: fetch cluster here.
            leaderStub =
                    this.state.stubs.values()
                            .toArray(VertxProtocolGrpc.ProtocolVertxStub[]::new)[0];
        }
        return task.apply(leaderStub);
    }

    int superQuorum(int size) {
        int faultTolerance = size - quorum(size);
        return faultTolerance + recoverQuorum(size);
    }

    int quorum(int size) {
        return size / 2 + 1;
    }

    int recoverQuorum(int size) {
        return quorum(size) / 2 + 1;
    }

    ProtocolClient(ClientConnectionManager connectionManager) {
        super(connectionManager);
        this.state = getInitState();
    }

    State getInitState() {
        ManagedChannel initChannel = this.connectionManager().getInitChannel();
        ProtocolGrpc.ProtocolBlockingStub initStub = ProtocolGrpc.newBlockingStub(initChannel);
        FetchClusterResponse response = null;
        do {
            try {
                response =
                        CompletableFuture.supplyAsync(
                                        () ->
                                                initStub.fetchCluster(
                                                        FetchClusterRequest.newBuilder()
                                                                .setLinearizable(false)
                                                                .build()),
                                        this.connectionManager().getExecutorService())
                                .get();
            } catch (Exception ignore) {
            }
        } while (response == null || !response.hasLeaderId());

        HashMap<Long, VertxProtocolGrpc.ProtocolVertxStub> stubs = new HashMap<>();
        for (Member member : response.getMembersList()) {
            String target =
                    member.getAddrsList().stream()
                            .map(URI::create)
                            .map(ProtocolClient::getEndpoint)
                            .distinct()
                            .collect(Collectors.joining(","));
            String authority = connectionManager().builder().authority();
            String ips =
                    String.format(
                            "%s://%s/%s",
                            IPNameResolver.SCHEME, authority != null ? authority : "", target);

            ManagedChannel channel = connectionManager().defaultChannelBuilder(ips).build();
            VertxProtocolGrpc.ProtocolVertxStub stub = VertxProtocolGrpc.newVertxStub(channel);
            stubs.put(member.getId(), stub);
        }

        return new State(
                response.getLeaderId(), response.getTerm(), response.getClusterVersion(), stubs);
    }

    private class State {
        private final ReadWriteLock lock;
        private long leaderId;
        private long term;
        private long clusterVersion;
        private HashMap<Long, VertxProtocolGrpc.ProtocolVertxStub> stubs;

        State(
                long leaderId,
                long term,
                long clusterVersion,
                HashMap<Long, VertxProtocolGrpc.ProtocolVertxStub> stubs) {
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

        HashMap<Long, VertxProtocolGrpc.ProtocolVertxStub> getStubs() {
            this.lock.readLock().lock();
            HashMap<Long, VertxProtocolGrpc.ProtocolVertxStub> stubs = this.stubs;
            this.lock.readLock().unlock();
            return stubs;
        }

        long getLeader() {
            this.lock.readLock().lock();
            long res = this.leaderId;
            this.lock.readLock().unlock();
            return res;
        }

        /**
         * Get leader stub
         *
         * @return Leader stub or null if there are some disjoints in local stubs.
         */
        @Nullable
        VertxProtocolGrpc.ProtocolVertxStub getLeaderStub() {
            this.lock.readLock().lock();
            VertxProtocolGrpc.ProtocolVertxStub stub = this.stubs.get(this.leaderId);
            this.lock.readLock().unlock();
            return stub;
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
                    logger().info("client term updates to " + this.term);
                    logger().info("client leader id updates to " + this.leaderId);
                }
                if (res.getClusterVersion() == this.clusterVersion) {
                    return;
                }
                this.clusterVersion = res.getClusterVersion();
                HashMap<Long, VertxProtocolGrpc.ProtocolVertxStub> stubs = new HashMap<>();
                for (Member member : res.getMembersList()) {
                    String target =
                            member.getAddrsList().stream()
                                    .map(URI::create)
                                    .map(ProtocolClient::getEndpoint)
                                    .distinct()
                                    .collect(Collectors.joining(","));
                    String authority = connectionManager().builder().authority();
                    String ips =
                            String.format(
                                    "%s://%s/%s",
                                    IPNameResolver.SCHEME,
                                    authority != null ? authority : "",
                                    target);

                    ManagedChannel channel = connectionManager().defaultChannelBuilder(ips).build();
                    VertxProtocolGrpc.ProtocolVertxStub stub =
                            VertxProtocolGrpc.newVertxStub(channel);
                    stubs.put(member.getId(), stub);
                }
                // TODO: do NOT drop the old stubs, instead modify the stubs (use ConcurrentHashMap)
                if (!stubs.isEmpty()) {
                    this.stubs = stubs;
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        }
    }

    static String getEndpoint(URI uri) {
        return uri.getHost() + (uri.getPort() != -1 ? ":" + uri.getPort() : "");
    }
}
