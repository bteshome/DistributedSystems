import com.bteshome.consistenthashing.Ring;

import java.util.List;

public class ConsistentHashingTest {
    public static void main(String[] args) {
        try {
            var ring = new Ring(10);
            var storage = new FakeInMemoryServerStorage();

            for (int i = 0; i < 5; i++) {
                ring.addServer("server" + i);
            }

            for (int i = 0; i < 70; i++) {
                var data = "data" + i;
                var server = ring.getServer(data);
                storage.store(server, data);
            }

            //ring.showRing();
            System.out.println("Storage before rebalance:");
            storage.showStorage();

            String serverToRemove = "server2";
            String serverToAdd = "server5";
            ring.removeServer(serverToRemove);
            ring.addServer(serverToAdd);
            List<String> server2Data = storage.removeAllData(serverToRemove);

            if (server2Data != null) {
                for (String dataItem : server2Data) {
                    var newServer = ring.getServer(dataItem);
                    storage.store(newServer, dataItem);
                }
            }

            //ring.showRing();
            System.out.println();
            System.out.println("Storage after rebalance:");
            storage.showStorage();
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }
}
