package cloud.xline.jxline.impl.blocking;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;

import java.util.ArrayList;

public class BlockingClient {
    private final BlockingKvClient kvClient;

    public BlockingClient(String[] addrs) {
        ArrayList<ManagedChannel> channels = new ArrayList<>();
        for (String addr : addrs) {
            ManagedChannel channel =
                    Grpc.newChannelBuilder(addr, InsecureChannelCredentials.create()).build();
            channels.add(channel);
        }
        BlockingProtocolClient curpClient = new BlockingProtocolClient(channels);

        this.kvClient = new BlockingKvClient(curpClient, "");
    }

    public BlockingKvClient getKVClient() {
        return this.kvClient;
    }
}
