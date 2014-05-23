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

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
/**
 * The singleton based instance provider for hazelcast instance.
 *
 * This is needed because there can be only one hazelcast instance.
 */
public class HazelcastInstanceManager {
    private static HazelcastInstanceManager instance = new HazelcastInstanceManager();
    private HazelcastInstance hazelcastInstance;

    public static HazelcastInstanceManager getInstance() {
        return instance;
    }

    public HazelcastInstance init(Config config) {
        if (hazelcastInstance != null) {
            throw new IllegalStateException("HazelcastInstanceManager has already been initialized");
        }
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        return hazelcastInstance;
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    private HazelcastInstanceManager() {
    }
}
