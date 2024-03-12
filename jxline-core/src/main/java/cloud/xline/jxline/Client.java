package cloud.xline.jxline;

public interface Client extends AutoCloseable {

    ProtocolClient getProtocolClient();

    Auth getAuthClient();

    KV getKVClient();

    Watch getWatchClient();

    /**
     * Override the jetcd.cloud.xline.client.Client.builder
     *
     * @return {@link ClientBuilder}
     */
    static ClientBuilder builder() {
        return new ClientBuilder();
    }
}
