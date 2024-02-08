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

    @BeforeAll
    static void onConnect() {
        client =
                Client.builder()
                        .endpoints("http://172.20.0.3:2379", "http://172.20.0.4:2379")
                        .build()
                        .getProtocolClient();
    }

    @Test
    void testWorks() throws Exception {
        Command command =
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
        PutResponse resp = client.propose(command, true, (sr, asr) -> sr.getPutResponse()).get();
        assertThat(resp).isNotNull();
    }
}
