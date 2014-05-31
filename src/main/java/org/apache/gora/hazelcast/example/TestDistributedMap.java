package main.java.org.apache.gora.hazelcast.example;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;



import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;



import main.java.org.apache.gora.hazelcast.store.HazelcastInstanceManager;
import main.java.org.apache.gora.hazelcast.store.HazelcastMapStore;
import main.java.org.apache.gora.hazelcast.store.MapEntryListener;

public class TestDistributedMap {
	public static void main(String[] args) {
		
		HazelcastInstanceManager hz=new HazelcastInstanceManager();
		Config cfg = new ClasspathXmlConfig("hazelcast.xml");
		
	
		
		HazelcastInstance instance=hz.init(cfg);
		HazelcastMapStore mapstore=new HazelcastMapStore(instance);
		
		Customer cus1=new Customer("ahsan","Sri Lanka",22);
		Customer cus2=new Customer("lewis","Scotland",30);
		Customer cus3=new Customer("renato","Brazil",25);
		
		MapEntryListener entrylistner= new MapEntryListener() {
			
			public <X> void entryUpdated(X key) {
				// TODO Auto-generated method stub
				
			}
			
			public <X> void entryRemoved(X key) {
				// TODO Auto-generated method stub
				
			}
			
			public <X> void entryAdded(X key) {
				// TODO Auto-generated method stub
				
			}
		};
		
		Map<String,Customer> customermap=mapstore.getMap("customer", entrylistner);
		customermap.put(cus1.getName(), cus1);
		customermap.put(cus2.getName(), cus2);
		customermap.put(cus3.getName(), cus3);
		
		System.out.println(customermap.size());
		System.out.println(customermap.keySet());
		System.out.println(customermap.values());
		System.out.println(customermap.containsKey("ahsan"));
		
		
		
		
		
	}

}
