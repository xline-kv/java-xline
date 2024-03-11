package cloud.xline.jxline.impl;

import cloud.xline.jxline.*;

import io.etcd.jetcd.support.MemorizingClientSupplier;

public final class ClientImpl implements Client {

    private final ClientConnectionManager manager;

    private final ProtocolClient protocolClient;

    private final MemorizingClientSupplier<KV> kvClient;

    private final MemorizingClientSupplier<Auth> authClient;

    public ClientImpl(ClientBuilder clientBuilder) {
        this.manager = new ClientConnectionManager(clientBuilder);
        this.protocolClient = new ProtocolClientImpl(this.manager);
        this.kvClient =
                new MemorizingClientSupplier<>(() -> new KVImpl(this.protocolClient, this.manager));
        this.authClient =
                new MemorizingClientSupplier<>(
                        () -> new AuthImpl(this.protocolClient, this.manager));
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
}
