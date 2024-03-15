package cloud.xline.jxline.impl;

import cloud.xline.jxline.Client;
import cloud.xline.jxline.ClientBuilder;

import cloud.xline.jxline.KV;
import cloud.xline.jxline.ProtocolClient;
import io.etcd.jetcd.support.MemorizingClientSupplier;

/** Xline client implementation. */
public final class ClientImpl implements Client {

    private final ClientConnectionManager manager;

    private final ProtocolClient protocolClient;

    private final MemorizingClientSupplier<KV> kvClient;

    /**
     * Create a new client instance.
     *
     * @param clientBuilder the builder to use for creating the client
     */
    public ClientImpl(ClientBuilder clientBuilder) {
        this.manager = new ClientConnectionManager(clientBuilder);
        this.protocolClient = new ProtocolClientImpl(this.manager);
        this.kvClient =
                new MemorizingClientSupplier<>(() -> new KVImpl(this.protocolClient, this.manager));
    }

    @Override
    public ProtocolClient getProtocolClient() {
        return this.protocolClient;
    }

    @Override
    public KV getKVClient() {
        return this.kvClient.get();
    }
}
