import cloud.xline.jxline.Client;
import cloud.xline.jxline.ProtocolClient;
import com.google.protobuf.ByteString;
import com.xline.protobuf.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.*;

@Timeout(value = 20)
public class ProtocolTest {
    static ProtocolClient client;

    static String INIT_ENDPOINT = "http://172.20.0.5:2379";

    @BeforeAll
    static void onConnect() {
        client = Client.builder().endpoints(INIT_ENDPOINT).build().getProtocolClient();
    }

    @Test
    void testItWorks() throws Exception {
        Command put =
                Command.newBuilder()
                        .setRequest(
                                RequestWithToken.newBuilder()
                                        .setPutRequest(
                                                PutRequest.newBuilder()
                                                        .setKey(ByteString.copyFromUtf8("Hello"))
                                                        .setValue(ByteString.copyFromUtf8("Xline"))
                                                        .build())
                                        .build())
                        .build();
        PutResponse putResp = client.propose(put, false, (sr, asr) -> sr.getPutResponse()).get();
        assertThat(putResp).isNotNull();
        Command get =
                Command.newBuilder()
                        .setRequest(
                                RequestWithToken.newBuilder()
                                        .setRangeRequest(
                                                RangeRequest.newBuilder()
                                                        .setKey(ByteString.copyFromUtf8("Hello"))
                                                        .build()))
                        .build();
        RangeResponse getResp = client.propose(get, true, (sr, asr) -> sr.getRangeResponse()).get();
        assertThat(getResp).isNotNull();
        assertThat(getResp.getCount()).isEqualTo(1);
        assertThat(getResp.getKvs(0).getValue()).isEqualTo(ByteString.copyFromUtf8("Xline"));
    }
}
