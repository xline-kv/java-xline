package cloud.xline.jxline.impl;

import com.curp.protobuf.*;
import io.grpc.ManagedChannel;

import java.net.URI;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

class ProtocolClient extends Impl {

    private final State state;

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
                    StreamSupport.stream(
                                    member.getAddrsList().stream().map(URI::create).spliterator(),
                                    false)
                            .map(e -> e.getHost() + (e.getPort() != -1 ? ":" + e.getPort() : ""))
                            .distinct()
                            .collect(Collectors.joining(","));
            ManagedChannel channel = this.connectionManager().defaultChannelBuilder(target).build();
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
                            StreamSupport.stream(
                                            member.getAddrsList().stream()
                                                    .map(URI::create)
                                                    .spliterator(),
                                            false)
                                    .map(
                                            e ->
                                                    e.getHost()
                                                            + (e.getPort() != -1
                                                                    ? ":" + e.getPort()
                                                                    : ""))
                                    .distinct()
                                    .collect(Collectors.joining(","));
                    ManagedChannel channel =
                            connectionManager().defaultChannelBuilder(target).build();
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
}
