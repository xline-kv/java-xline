package cloud.xline.jxline.impl;

import cloud.xline.jxline.*;

import io.etcd.jetcd.support.MemorizingClientSupplier;

public final class ClientImpl implements Client {

    private final ClientConnectionManager manager;

    private final ProtocolClient protocolClient;

    private final MemorizingClientSupplier<KV> kvClient;

    private final MemorizingClientSupplier<Auth> authClient;

    private final MemorizingClientSupplier<Watch> watchClient;

    public ClientImpl(ClientBuilder clientBuilder) {
        this.manager = new ClientConnectionManager(clientBuilder);
        this.protocolClient = new ProtocolClientImpl(this.manager);
        this.kvClient =
                new MemorizingClientSupplier<>(() -> new KVImpl(this.protocolClient, this.manager));
        this.authClient =
                new MemorizingClientSupplier<>(
                        () -> new AuthImpl(this.protocolClient, this.manager));
        this.watchClient = new MemorizingClientSupplier<>(() -> new WatchImpl(this.manager));
    }

    @Override
    public ProtocolClient getProtocolClient() {
        return this.protocolClient;
    }

    @Override
    public KV getKVClient() {
        return this.kvClient.get();
    }

    @Override
    public Auth getAuthClient() {
        return this.authClient.get();
    }

    @Override
    public Watch getWatchClient() {
        return this.watchClient.get();
    }

    @Override
    public void close() {
        this.kvClient.close();
        this.authClient.close();
        this.watchClient.close();
        this.protocolClient.close();
        this.manager.close();
    }
}
