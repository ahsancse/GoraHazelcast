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

/**
 * @author Ahsan Shamsudeen
 */
public class HazelcastStoreConstants {
	
	public HazelcastStoreConstants(){
		
	}

  /**
   * The mapping file to create the tables from
   */
  public static final String DEFAULT_MAPPING_FILE = "gora-hazelcast-mapping.xml";
  public static final String DEFAULT_CONFIGURATION_FILE = "hazelcast.xml";

  /**
   * The names of the properties in the gora.properties file.
   */
  
  public static final String STORE_NAME = "storename";
  public static final String PRIMARYKEY_TABLE_NAME = "primarykey_tablename";
  public static final String DEFAULT_STORE_NAME = "hazelcaststore";
  public static final String DEFAULT_PRIMARYKEY_TABLE_NAME = "PrimaryKeys";
  public static final String PROPERTIES_SEPARATOR = ",";
}
