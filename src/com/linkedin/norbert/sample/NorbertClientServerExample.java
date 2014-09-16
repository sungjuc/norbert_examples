package com.linkedin.norbert.sample;

import com.linkedin.norbert.cluster.InvalidClusterException;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.ClusterListener;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.cluster.ZooKeeperClusterClient;
import com.linkedin.norbert.javacompat.network.*;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class NorbertClientServerExample {
    public static void main(String[] args) {
        final String serviceName = "norbert";
        final String zkConnectStr = "localhost:2181";

        // ZooKeeper cluster configurations
        configCluster(serviceName, zkConnectStr);

        // Start two norbert server
        startServer(serviceName, 1, zkConnectStr);
        startServer(serviceName, 2, zkConnectStr);

        // Generate request to norbert server using round-robin load balancer.
        NetworkClientConfig config = new NetworkClientConfig();
        config.setServiceName(serviceName);
        config.setZooKeeperConnectString(zkConnectStr);
        config.setZooKeeperSessionTimeoutMillis(30000);
        config.setConnectTimeoutMillis(1000);
        config.setWriteTimeoutMillis(150);
        config.setMaxConnectionsPerNode(5);
        config.setStaleRequestTimeoutMins(10);
        config.setStaleRequestCleanupFrequencyMins(10);

        NetworkClient nc = new NettyNetworkClient(config, new RoundRobinLoadBalancerFactory());

        final Ping request1 = new Ping(System.currentTimeMillis());
        final Ping request2 = new Ping(System.currentTimeMillis());
        Future<Ping> pingFuture = nc.sendRequest(request1, new PingSerializer());

        try {
            final Ping appendResp = pingFuture.get();
            System.out.println("[NettyNetworkClient] got response: " + appendResp);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        pingFuture = nc.sendRequest(request2, new PingSerializer());

        try {
            final Ping appendResp = pingFuture.get();
            System.out.println("[NettyNetworkClient] got response: " + appendResp);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

    private static void startServer(String serviceName, int nodeId, String zkConnectStr) {
        NetworkServerConfig config = new NetworkServerConfig();
        config.setServiceName(serviceName);
        config.setZooKeeperConnectString(zkConnectStr);
        config.setZooKeeperSessionTimeoutMillis(30000);
        config.setRequestThreadCorePoolSize(5);
        config.setRequestThreadMaxPoolSize(10);
        config.setRequestThreadKeepAliveTimeSecs(300);

        NetworkServer ns = new NettyNetworkServer(config);

        ns.registerHandler(new RequestHandler<Ping, Ping>() {
            @Override
            public Ping handleRequest(Ping request) throws Exception {
                System.out.println("[NetworkServer] got request: " + request);
                return new Ping(System.currentTimeMillis());
            }
        }, new PingSerializer());


        ns.bind(nodeId);
    }

    private static void configCluster(String serviceName, String zkConnectStr) {
        final ClusterClient cc = new ZooKeeperClusterClient(serviceName, zkConnectStr, 30000);
        cc.awaitConnectionUninterruptibly();

        cc.addListener(new ClusterListener() {
            @Override
            public void handleClusterConnected(Set<Node> nodes) {
                System.out.println("[ClusterListener] connected to cluster: " + nodes);
            }

            @Override
            public void handleClusterNodesChanged(Set<Node> nodes) {
                System.out.println("[ClusterListener] nodes changed: " + nodes);
            }

            @Override
            public void handleClusterDisconnected() {
                final Set<Node> nodes = cc.getNodes();
                System.out.println("[ClusterListener] dis-connected from cluster: " + nodes);
            }

            @Override
            public void handleClusterShutdown() {
                final Set<Node> nodes = cc.getNodes();
                System.out.println("[ClusterListener] cluster shutdown: " + nodes);
            }
        });

        cc.removeNode(1);
        cc.removeNode(2);
        cc.addNode(1, "localhost:5002");
        cc.addNode(2, "localhost:5003");
    }
}
