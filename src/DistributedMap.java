import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class DistributedMap implements SimpleStringMap {
    private ConcurrentHashMap<String, String> concurrentMap;
    private JGroups jGroups;
    public DistributedMap(){
        concurrentMap= new ConcurrentHashMap<String,String>();
        jGroups=new JGroups(concurrentMap);
    }

    @Override
    public boolean containsKey(String key) {
        return concurrentMap.containsKey(key);
    }

    @Override
    public String get(String key) {
        return concurrentMap.get(key);
    }

    @Override
    public String put(String key, String value) {
        try {
            jGroups.send("put " +key+" "+value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return value;
    }

    @Override
    public String remove(String key) {
        String value = concurrentMap.get(key);
        try {
            jGroups.send("remove "+key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return value;
    }
}
