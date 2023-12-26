package cloud.xline.client;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;

import java.util.ArrayList;


public class Client {
    public KvClient kvClient;

    public Client(String[] addrs) {
        ArrayList<ManagedChannel> channels = new ArrayList<>();
        for (String addr : addrs) {
            ManagedChannel channel = Grpc.newChannelBuilder(addr, InsecureChannelCredentials.create()).build();
            channels.add(channel);
        }
        ProtocolClient curpClient = new ProtocolClient(channels);

        this.kvClient = new KvClient(curpClient, "");
    }
}
