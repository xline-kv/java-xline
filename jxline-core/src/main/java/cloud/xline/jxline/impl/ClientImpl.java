package cloud.xline.jxline.impl;

import cloud.xline.jxline.Client;
import cloud.xline.jxline.ClientBuilder;

import io.etcd.jetcd.*;

public final class ClientImpl implements Client {

    public ClientImpl(ClientBuilder clientBuilder) {}

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
    public void close() {}
}
