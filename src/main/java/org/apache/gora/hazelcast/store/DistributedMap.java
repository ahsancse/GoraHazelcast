/*
 Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package main.java.org.apache.gora.hazelcast.store;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.spi.Operation;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public  class DistributedMap<K, V>  implements Map<K, V>  {
	
    private HazelcastInstance hazelcastInstance;                  //reference to the Hazelcast NoSQL datastore
	private IMap<K, V> map;
	    
	public HazelcastInstance getHazelcastInstance(){              //return HazelcastInstance
        return hazelcastInstance;
    }
        
    public IMap<K, V> getDistributedMap(){
        return map;
    }

    public int size() {                                               // return size of the map
        if (hazelcastInstance.getLifecycleService().isRunning()) {
            return map.size();
        }
        return 0;
    }

    public boolean isEmpty() {                                          // check whether map is empty
        if(hazelcastInstance.getLifecycleService().isRunning()){
            return map.isEmpty();
        }
        return true;
    }

    public boolean containsKey(Object key) {
        return hazelcastInstance.getLifecycleService().isRunning() && map.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return hazelcastInstance.getLifecycleService().isRunning() && map.containsValue(value);
    }

    public V get(Object key) {
        if (hazelcastInstance.getLifecycleService().isRunning()) {
            return map.get(key);
        }
        return null;
    }

    public V put(K key, V value) {
        if (hazelcastInstance.getLifecycleService().isRunning()) {
            return map.put(key, value);
        }
        return value;
    }

    public V remove(Object key) {
        if (hazelcastInstance.getLifecycleService().isRunning()) {
            return map.remove(key);
        }
        return null;
    }

    public void putAll(Map<? extends K, ? extends V> m) {
        if (hazelcastInstance.getLifecycleService().isRunning()) {
            map.putAll(m);
        }
    }

    public void clear() {
        if (hazelcastInstance.getLifecycleService().isRunning()) {
            map.clear();
        }
    }

    public Set<K> keySet() {
        if (hazelcastInstance.getLifecycleService().isRunning()) {
            return map.keySet();
        }
        return new LinkedHashSet<K>();
    }

    public Collection<V> values() {
        if (hazelcastInstance.getLifecycleService().isRunning()) {
            return map.values();
        }
        return new ArrayList<V>();
    }

    public Set<Entry<K, V>> entrySet() {
        if (hazelcastInstance.getLifecycleService().isRunning()) {
            return map.entrySet();
        }
        return new LinkedHashSet<Entry<K, V>>();
    }
}
