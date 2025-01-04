import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FakeInMemoryServerStorage {
    private final HashMap<String, List<String>> storage = new HashMap<>();

    public void store(String server, String data) {
        if (!storage.containsKey(server)) {
            storage.put(server, new ArrayList<>());
        }
        storage.get(server).add(data);
    }

    public List<String> removeAllData(String server) {
        return storage.remove(server);
    }

    public void showStorage() {
        for (var server : storage.entrySet()) {
            System.out.println("Storage for server " + server.getKey() + ": " + server.getValue());
        }
    }
}
