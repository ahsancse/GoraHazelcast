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

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;




import java.util.*;

public class HazelcastMapStore implements DistributedMapStore {

    private HazelcastInstance hazelcastInstance;
    @SuppressWarnings("rawtypes")
	private Map<String, DistributedMap> maps = new HashMap<String, DistributedMap>();

    public HazelcastMapStore(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    public <K, V> Map<K, V> getMap(String mapName, final MapEntryListener entryListener) {
    	@SuppressWarnings("unchecked")
		DistributedMap<K,V> map = maps.get(mapName);
        if(map == null){
            map = new DistributedMap<K, V>(mapName, entryListener);
            maps.put(mapName, map);
        }
        return map;
    }

    public void removeMap(String mapName) {
        maps.remove(mapName);
        hazelcastInstance.getMap(mapName).flush();
    }

    private class DistributedMap<K, V> implements Map<K, V> {
        private Map<K, V> map;

        public DistributedMap(String mapName, final MapEntryListener entryListener) {
            this.map = hazelcastInstance.getMap(mapName);
            if (entryListener != null) {
                ((IMap<K, V>) map).addEntryListener(new EntryListener<K, V>() {
                    public void entryAdded(EntryEvent<K, V> kvEntryEvent) {
                        entryListener.entryAdded(kvEntryEvent.getKey());
                    }

                    public void entryRemoved(EntryEvent<K, V> kvEntryEvent) {
                        entryListener.entryRemoved(kvEntryEvent.getKey());
                    }

                    public void entryUpdated(EntryEvent<K, V> kvEntryEvent) {
                        entryListener.entryUpdated(kvEntryEvent.getKey());
                    }

                    public void entryEvicted(EntryEvent<K, V> kvEntryEvent) {
                        entryListener.entryRemoved(kvEntryEvent.getKey());
                    }
                }, false);
            }
        }

        public int size() {
            if (hazelcastInstance.getLifecycleService().isRunning()) {
                return map.size();
            }
            return 0;
        }

        public boolean isEmpty() {
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
}