package main.java.org.apache.gora.hazelcast.example;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;





import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;





import main.java.org.apache.gora.hazelcast.store.HazelcastInstanceManager;
import main.java.org.apache.gora.hazelcast.store.DistributedMap;
import main.java.org.apache.gora.hazelcast.store.MapEntryListener;

public class TestDistributedMap {
	public static void main(String[] args) {
		
		/*HazelcastInstanceManager hz=new HazelcastInstanceManager();
		Config cfg = new ClasspathXmlConfig("hazelcast.xml");
		HazelcastInstance instance=hz.init(cfg);
		DistributedMap<String, Customer> mapstore=new DistributedMap<String, Customer>(instance,"customer");
		
		Customer cus1=new Customer("ahsan","Sri Lanka",22);
		Customer cus2=new Customer("lewis","Scotland",30);
		Customer cus3=new Customer("renato","Brazil",25);
		
		//Map<String,Customer> customermap=mapstore.get("customer");
		mapstore.put(cus1.getName(), cus1);
		mapstore.put(cus2.getName(), cus2);
		mapstore.put(cus3.getName(), cus3);
		
		System.out.println(mapstore.size());
		System.out.println(mapstore.keySet());
		System.out.println(mapstore.values());
		System.out.println(mapstore.containsKey("ahsan"));*/
		
	}

}
