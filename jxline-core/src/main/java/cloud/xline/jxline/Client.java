package cloud.xline.jxline;

public interface Client {

    ProtocolClient getProtocolClient();

    Auth getAuthClient();

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
