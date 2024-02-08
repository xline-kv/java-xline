package cloud.xline.jxline.impl;

import cloud.xline.jxline.Client;
import cloud.xline.jxline.ClientBuilder;

import cloud.xline.jxline.ProtocolClient;
import io.etcd.jetcd.*;

public final class ClientImpl implements Client {

    private final ClientConnectionManager manager;

    public ClientImpl(ClientBuilder clientBuilder) {
        this.manager = new ClientConnectionManager(clientBuilder);
    }

    @Override
    public Auth getAuthClient() {
        return null;
    }

    @Override
    public KV getKVClient() {
        return null;
    }

    @Override
    public Cluster getClusterClient() {
        return null;
    }

    @Override
    public Maintenance getMaintenanceClient() {
        return null;
    }

    @Override
    public Lease getLeaseClient() {
        return null;
    }

    @Override
    public Watch getWatchClient() {
        return null;
    }

    @Override
    public Lock getLockClient() {
        return null;
    }

    @Override
    public Election getElectionClient() {
        return null;
    }

    @Override
    public ProtocolClient getProtocolClient() {
        return new ProtocolClientImpl(this.manager);
    }

    @Override
    public void close() {}
}
