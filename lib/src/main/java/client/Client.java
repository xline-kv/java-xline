package client;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;

import java.util.ArrayList;


public class Client {
    public KvClient kvClient;

    public Client(String[] addrs) {
        ArrayList<ManagedChannel> channels = new ArrayList<ManagedChannel>();
        for (String addr : addrs) {
            ManagedChannel channel = Grpc.newChannelBuilder(addr, InsecureChannelCredentials.create()).build();
            channels.add(channel);
        }
        ProtocolClient curpClient = new ProtocolClient(channels);

        KvClient kvClient = new KvClient("client", curpClient, "");

        this.kvClient = kvClient;
    }
}
