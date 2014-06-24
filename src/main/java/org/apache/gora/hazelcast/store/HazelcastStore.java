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

import main.java.org.apache.gora.hazelcast.store.HazelcastMapping.HazelcastMappingBuilder;
import main.java.org.apache.gora.hazelcast.utils.Encoder;
//import main.java.org.apache.gora.hazelcast.utils.BinaryEncoder;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.store.impl.DataStoreBase;
import org.apache.gora.util.AvroUtils;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadPendingException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.hibernate.local.Value;
import com.hazelcast.spi.Operation;


public class HazelcastStore<K,T extends PersistentBase> extends DataStoreBase<K,T> {
	
	   /**
	   * Helper to write useful information into the logs
	   */
    private static final Logger LOG = LoggerFactory.getLogger(HazelcastStore.class);
	

	private HazelcastMapping mapping;                                      //the mapping to the datastore
	private Encoder encoder;                                                   // the serialisation encoder
	  /*********************************************************************
	   * Variables and references to Hazelcast NoSQL properties
	   * and configuration values.
	   *********************************************************************/
	private static String storeName;                                               //the name of the Hazelcast key-value store
	private static String mappingFile;                                             //the filename of the mapping (xml) file
	private static String configurationFile;
	private static String primaryKeyTable;                                         //the name of the table that stores the primary keys
	
	/**
    Set of operations to be executed during flush().
    It is a LinkedHashSet in order to retain the order in which
    each operation was added to the collection.
	*/
	LinkedHashSet<List<Operation>> operations;
	private HazelcastInstance hazelcastInstance;                                  // reference to the Hazelcast datastore
	//private DistributedMap<K,T> map;
	private IMap<K, T> map;
	
	
	/**
	   * Initialize the data store by initialising the operations, setting the datastore
	   * setting the datastore properties up, and reading the mapping file
	*/
	  
	@Override
	public void initialize(Class<K> keyClass, Class<T> persistentClass, Properties properties){
	    super.initialize(keyClass, persistentClass, properties);
		
		if ((mapping != null) && (hazelcastInstance != null)){
		      LOG.warn("HazelcastStore is already initialised");
		      return;
		}
		
		operations = new LinkedHashSet();
		encoder=new main.java.org.apache.gora.hazelcast.utils.BinaryEncoder();
		readProperties(properties);
		setupStore();
		
		try{
		    LOG.debug("mappingFile="+mappingFile);
		    mapping = readMapping( mappingFile );
		}
		catch ( IOException e ) {
		    LOG.error( e.getMessage() );
		    LOG.error( e.getStackTrace().toString());
		}

		if(autoCreateSchema) {
		    createSchema();
		}
	}
	
	/**
	   * Sets the configuration for the client according to the properties
	   * and establishes a new Hazelcast  NoSQL datastore.
	*/
	private void setupStore(){
		HazelcastInstanceManager hz=new HazelcastInstanceManager();
		Config cfg = new ClasspathXmlConfig(configurationFile);
		hazelcastInstance=hz.init(cfg);
		map=hazelcastInstance.getMap("customer");
	}
	
	public void readProperties(Properties properties) {
		mappingFile = DataStoreFactory.getMappingFile(properties, this, HazelcastStoreConstants.DEFAULT_MAPPING_FILE);
		configurationFile=DataStoreFactory.getMappingFile(properties, this, HazelcastStoreConstants.DEFAULT_CONFIGURATION_FILE);
		storeName = DataStoreFactory.findProperty(properties, this, HazelcastStoreConstants.STORE_NAME, HazelcastStoreConstants.DEFAULT_STORE_NAME);
		primaryKeyTable = DataStoreFactory.findProperty(properties, this, HazelcastStoreConstants.PRIMARYKEY_TABLE_NAME, HazelcastStoreConstants.DEFAULT_PRIMARYKEY_TABLE_NAME);
		System.out.println(mappingFile);
	}
	
	private HazelcastMapping readMapping(String mappingFilename) throws IOException {

	    HazelcastMappingBuilder mappingBuilder = new HazelcastMapping.HazelcastMappingBuilder();

	    try {
	      SAXBuilder builder = new SAXBuilder();
	      LOG.debug("about to parse: "+mappingFilename);
	      InputStream mappingFile = getClass().getClassLoader().getResourceAsStream(mappingFilename);

	      if (mappingFile==null){
	        LOG.error("mappingFile is null");
	        throw new IOException("Unable to open "+mappingFilename);
	      }

	      Document doc = builder.build(mappingFile);

	      List<Element> classes = doc.getRootElement().getChildren("class");

	      for ( Element classElement : classes ) {

	        if ( classElement.getAttributeValue("keyClass").equals( keyClass.getCanonicalName())
	                && classElement.getAttributeValue("name").equals( persistentClass.getCanonicalName())) {

	          String tableName = getSchemaName( classElement.getAttributeValue("table"), persistentClass );
	          mappingBuilder.setTableName(tableName);

	          mappingBuilder.setClassName( classElement.getAttributeValue("name") );
	          mappingBuilder.setKeyClass( classElement.getAttributeValue("keyClass") );

	          Element primaryKeyEl = classElement.getChild("primarykey");

	          String primaryKeyField = primaryKeyEl.getAttributeValue("name");
	          String primaryKeyColumn = primaryKeyEl.getAttributeValue("column");

	          mappingBuilder.setPrimaryKey( primaryKeyField );
	          mappingBuilder.addField(primaryKeyField, primaryKeyColumn);

	          List<Element> fields = classElement.getChildren("field");

	          for ( Element field : fields ) {
	            String fieldName = field.getAttributeValue("name");
	            String columnName = field.getAttributeValue("column");

	            mappingBuilder.addField(fieldName, columnName);
	          }
	          break;
	        }
	      }

	    }
	    catch ( Exception ex ) {
	      LOG.error("Error in parsing: "+ex.getMessage());
	      throw new IOException(ex);
	    }

	    LOG.debug("parse finished.");
	    return mappingBuilder.build();
	  }
	
	/**
	   * Wrapper method for the extended fromBytes method. It uses the default encoder.
	   * @param schema  The schema type of the data
	   * @param data  the data
	   * @return  the deserialised Object
	   */
	  public Object fromBytes(Schema schema, byte data[]) {
	    return fromBytes(encoder, schema, data);
	  }

	  /**
	   * Method for deserialising serialised values.
	   * @param encoder The encoder to use
	   * @param schema  The schema type of the data
	   * @param data  the data
	   * @return  the deserialised Object
	   */
	  public static Object fromBytes(Encoder encoder, Schema schema, byte data[]) {
	    switch (schema.getType()) {
	      case BOOLEAN:
	        return encoder.decodeBoolean(data);
	      case DOUBLE:
	        return encoder.decodeDouble(data);
	      case FLOAT:
	        return encoder.decodeFloat(data);
	      case INT:
	        return encoder.decodeInt(data);
	      case LONG:
	        return encoder.decodeLong(data);
	      case STRING:
	        return new Utf8(data);
	      case BYTES:
	        return ByteBuffer.wrap(data);
	      case ENUM:
	        return AvroUtils.getEnumValue(schema, encoder.decodeInt(data));
	      default:
			break;
	    }
	    throw new IllegalArgumentException("Unknown type " + schema.getType());

	  }

	  /**
	   * Gets the name of the table that stores the primary keys.
	   * @return the name of the table that stores the primary keys.
	   */
	  public static String getPrimaryKeyTable() {
	    return primaryKeyTable;
	  }
	
	
	
	public void close() {
	}
	
	public boolean schemaExists() {
		//return hazelcastInstance.get(mapping.getMajorKey())!=null ? true : false;
		return map.containsKey(mapping.getMajorKey()!=null ? true : false);
		
	}

	public void createSchema() {
		if (schemaExists()){
		      return;
		}
		
		int tries=0;
		while (tries<2){
		      try {
		    	  
		        map.put(mapping.getMajorKey(),null);
		        tries=2;
		        LOG.debug("Schema: "+mapping.getMajorKey()+" was created successfully");
		      } catch (OperationTimeoutException ote) {
		        // The durability guarantee could not be met.
		        if (tries==1)
		          LOG.error( ote.getMessage(), ote.getStackTrace().toString() );
		        else {
		          LOG.warn("DurabilityException occurred. Retrying one more time after 200 ms.");
		          try {
		            Thread.sleep(200);
		          } catch (InterruptedException e) {
		            e.printStackTrace();
		          }
		          tries++;
		          continue;
		        }
		      } catch (TimeoutException te) {
		        // The operation was not completed inside of the
		        // default request timeout limit.
		        if (tries==1)
		          LOG.error( te.getMessage(), te.getStackTrace().toString() );
		        else {
		          LOG.warn("RequestTimeoutException occurred. Retrying one more time after 200 ms.");
		          try {
		            Thread.sleep(200);
		          } catch (InterruptedException e) {
		            e.printStackTrace();
		          }
		          tries++;
		          continue;
		        }
		      } catch ( Exception e) {
		        // A generic error occurred
		        if (tries==1)
		          LOG.error( e.getMessage(), e.getStackTrace().toString() );
		        else {
		          LOG.warn("FaultException occurred. Retrying one more time after 200 ms.");
		          try {
		            Thread.sleep(200);
		          } catch (InterruptedException ie) {
		            ie.printStackTrace();
		          }
		          tries++;
		          continue;
		        }
		      }
		    }
		
		
		
		
		
	}

	public boolean delete(K arg0) {
		
		return false;
	}

	public long deleteByQuery(Query<K, T> arg0) {
		
		return 0;
	}

	public void deleteSchema() {
		
		
	}

	public Result<K, T> execute(Query<K, T> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	public void flush() {
		// TODO Auto-generated method stub
		
	}

	public T get(K arg0, String[] arg1) {
		
		return null;
	}

	public List<PartitionQuery<K, T>> getPartitions(Query<K, T> arg0)
			throws IOException {
		
		return null;
	}

	public String getSchemaName() {
		return mapping.getTableName();
	}

	public Query<K, T> newQuery() {
		
		return null;
	}

	public void put(K arg0, T arg1) {
		
		
	}
	
	

	
	
}
