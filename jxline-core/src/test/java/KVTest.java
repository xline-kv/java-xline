import cloud.xline.jxline.Client;
import cloud.xline.jxline.KV;
import cloud.xline.jxline.kv.DeleteResponse;
import cloud.xline.jxline.kv.GetResponse;
import cloud.xline.jxline.kv.PutResponse;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.assertj.core.api.Assertions.*;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Timeout(value = 20)
public class KVTest {
    private static KV kvClient;

    private static final ByteSequence SAMPLE_KEY = bytesOf("sample_key");
    private static final ByteSequence SAMPLE_VALUE = bytesOf("sample_value");
    private static final ByteSequence SAMPLE_KEY_2 = bytesOf("sample_key2");
    private static final ByteSequence SAMPLE_VALUE_2 = bytesOf("sample_value2");
    private static final ByteSequence SAMPLE_KEY_3 = bytesOf("sample_key3");

    @BeforeAll
    static void onConnect() {
        kvClient = Client.builder().endpoints("http://172.20.0.5:2379").build().getKVClient();
    }

    public static ByteSequence bytesOf(final String string) {
        return ByteSequence.from(string, StandardCharsets.UTF_8);
    }

    public static String randomString() {
        return java.util.UUID.randomUUID().toString();
    }

    @Test
    void testItWorks() throws Exception {
        ByteSequence key = ByteSequence.from("Hello Xline", Charset.defaultCharset());
        ByteSequence value = ByteSequence.from("Hi", Charset.defaultCharset());
        PutResponse putResponse = kvClient.put(key, value).get();
        assertThat(putResponse).isNotNull();
        GetResponse getResponse = kvClient.get(key).get();
        assertThat(getResponse).isNotNull();
        assertThat(getResponse.getCount()).isEqualTo(1);
        assertThat(getResponse.getKvs().get(0).getValue()).isEqualTo(value);
    }

    @Test
    public void testByteSequence() {
        ByteSequence prefix = bytesOf("/test-service/");
        ByteSequence subPrefix = bytesOf("uuids/");

        String keyString = randomString();
        ByteSequence key = bytesOf(keyString);
        ByteSequence prefixedKey = prefix.concat(subPrefix).concat(key);
        assertThat(prefixedKey.startsWith(prefix)).isTrue();
        assertThat(
                        prefixedKey
                                .substring(prefix.size() + subPrefix.size())
                                .toString(StandardCharsets.UTF_8))
                .isEqualTo(keyString);
        assertThat(prefixedKey.substring(prefix.size(), prefix.size() + subPrefix.size()))
                .isEqualTo(subPrefix);
    }

    @Test
    public void testPut() throws Exception {
        CompletableFuture<PutResponse> feature = kvClient.put(SAMPLE_KEY, SAMPLE_VALUE);
        PutResponse response = feature.get();
        assertThat(response.getHeader()).isNotNull();
        assertThat(!response.hasPrevKv()).isTrue();
    }

    @Test
    public void testGet() throws Exception {
        CompletableFuture<PutResponse> feature = kvClient.put(SAMPLE_KEY_2, SAMPLE_VALUE_2);
        feature.get();
        CompletableFuture<GetResponse> getFeature = kvClient.get(SAMPLE_KEY_2);
        GetResponse response = getFeature.get();
        assertThat(response.getKvs()).hasSize(1);
        assertThat(response.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8))
                .isEqualTo(SAMPLE_VALUE_2.toString(StandardCharsets.UTF_8));
        assertThat(!response.isMore()).isTrue();
    }

    @Test
    public void testGetWithRev() throws Exception {
        CompletableFuture<PutResponse> feature = kvClient.put(SAMPLE_KEY_3, SAMPLE_VALUE);
        PutResponse putResp = feature.get();
        kvClient.put(SAMPLE_KEY_3, SAMPLE_VALUE_2).get();
        GetOption option =
                GetOption.builder().withRevision(putResp.getHeader().getRevision()).build();
        CompletableFuture<GetResponse> getFeature = kvClient.get(SAMPLE_KEY_3, option);
        GetResponse response = getFeature.get();
        assertThat(response.getKvs()).hasSize(1);
        assertThat(response.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8))
                .isEqualTo(SAMPLE_VALUE.toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testGetSortedPrefix() throws Exception {
        String prefix = randomString();
        int numPrefix = 3;
        putKeysWithPrefix(prefix, numPrefix);

        GetOption option =
                GetOption.builder()
                        .withSortField(GetOption.SortTarget.KEY)
                        .withSortOrder(GetOption.SortOrder.DESCEND)
                        .isPrefix(true)
                        .build();
        CompletableFuture<GetResponse> getFeature = kvClient.get(bytesOf(prefix), option);
        GetResponse response = getFeature.get();

        assertThat(response.getKvs()).hasSize(numPrefix);
        for (int i = 0; i < numPrefix; i++) {
            assertThat(response.getKvs().get(i).getKey().toString(StandardCharsets.UTF_8))
                    .isEqualTo(prefix + (numPrefix - i - 1));
            assertThat(response.getKvs().get(i).getValue().toString(StandardCharsets.UTF_8))
                    .isEqualTo(String.valueOf(numPrefix - i - 1));
        }
    }

    @Test
    public void testDelete() throws Exception {
        // Put content so that we actually have something to delete
        testPut();

        ByteSequence keyToDelete = SAMPLE_KEY;

        // count keys about to delete
        CompletableFuture<GetResponse> getFeature = kvClient.get(keyToDelete);
        GetResponse resp = getFeature.get();

        // delete the keys
        CompletableFuture<DeleteResponse> deleteFuture = kvClient.delete(keyToDelete);
        DeleteResponse delResp = deleteFuture.get();
        assertThat(delResp.getDeleted()).isEqualTo(resp.getKvs().size());
    }

    @Test
    public void testGetAndDeleteWithPrefix() throws Exception {
        String prefix = randomString();
        ByteSequence key = bytesOf(prefix);
        int numPrefixes = 10;

        putKeysWithPrefix(prefix, numPrefixes);

        // verify get withPrefix.
        CompletableFuture<GetResponse> getFuture =
                kvClient.get(key, GetOption.builder().isPrefix(true).build());
        GetResponse getResp = getFuture.get();
        assertThat(getResp.getCount()).isEqualTo(numPrefixes);

        // verify del withPrefix.
        DeleteOption deleteOpt = DeleteOption.builder().isPrefix(true).build();
        CompletableFuture<DeleteResponse> delFuture = kvClient.delete(key, deleteOpt);
        DeleteResponse delResp = delFuture.get();
        assertThat(delResp.getDeleted()).isEqualTo(numPrefixes);
    }

    private static void putKeysWithPrefix(String prefix, int numPrefixes)
            throws ExecutionException, InterruptedException {
        for (int i = 0; i < numPrefixes; i++) {
            ByteSequence key = bytesOf(prefix + i);
            ByteSequence value = bytesOf("" + i);
            kvClient.put(key, value).get();
        }
    }
}
