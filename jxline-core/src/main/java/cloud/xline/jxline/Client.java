package cloud.xline.jxline;

/**
 * Xline client
 */
public interface Client {

    /**
     * Get the protocol client
     *
     * @return the {@link ProtocolClient}
     */
    ProtocolClient getProtocolClient();

    /**
     * Get the kv client
     *
     * @return the {@link KV}
     */
    KV getKVClient();

    /**
     * Override the jetcd.cloud.xline.client.Client.builder
     *
     * @return {@link ClientBuilder}
     */
    static ClientBuilder builder() {
        return new ClientBuilder();
    }
}
