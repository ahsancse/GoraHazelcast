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
import org.apache.hadoop.http.HttpServer.QuotingInputFilter.RequestQuoter;

import java.io.FileNotFoundException;
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
import com.hazelcast.multimap.operations.ValuesOperation;
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
	private IMap<String,String> map;
	
	
	/**
	   * Initialize the data store by initialising the operations, setting the datastore
	   * setting the datastore properties up, and reading the mapping file
	*/
	  
	@Override
	public void initialize(Class<K> keyClass, Class<T> persistentClass, Properties properties){
	    try {
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
			
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
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
		map=hazelcastInstance.getMap(storeName);
	}
	
	public void readProperties(Properties properties) {
		try {
			mappingFile = DataStoreFactory.getMappingFile(properties, this, HazelcastStoreConstants.DEFAULT_MAPPING_FILE);
			configurationFile=DataStoreFactory.getMappingFile(properties, this, HazelcastStoreConstants.DEFAULT_CONFIGURATION_FILE);
			storeName = DataStoreFactory.findProperty(properties, this, HazelcastStoreConstants.STORE_NAME, HazelcastStoreConstants.DEFAULT_STORE_NAME);
			primaryKeyTable = DataStoreFactory.findProperty(properties, this, HazelcastStoreConstants.PRIMARYKEY_TABLE_NAME, HazelcastStoreConstants.DEFAULT_PRIMARYKEY_TABLE_NAME);
			System.out.println(mappingFile);
		}catch (Exception e) {
			LOG.error(e.getMessage(), e);  
		}
		
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
	  
	  public K fromBytes(Class<K> clazz, byte[] val) {
		    return fromBytes(encoder, clazz, val);
      }
	  
	  @SuppressWarnings("unchecked")
      public static <K> K fromBytes(Encoder encoder, Class<K> clazz, byte[] val) {
	      try{
		      if (clazz.equals(Byte.TYPE) || clazz.equals(Byte.class)) {
		          return (K) Byte.valueOf(encoder.decodeByte(val));
		      }else if (clazz.equals(Boolean.TYPE) || clazz.equals(Boolean.class)) {
		          return (K) Boolean.valueOf(encoder.decodeBoolean(val));
		      } else if (clazz.equals(Short.TYPE) || clazz.equals(Short.class)) {
		          return (K) Short.valueOf(encoder.decodeShort(val));
		      } else if (clazz.equals(Integer.TYPE) || clazz.equals(Integer.class)) {
		          return (K) Integer.valueOf(encoder.decodeInt(val));
		      } else if (clazz.equals(Long.TYPE) || clazz.equals(Long.class)) {
		          return (K) Long.valueOf(encoder.decodeLong(val));
		      } else if (clazz.equals(Float.TYPE) || clazz.equals(Float.class)) {
		          return (K) Float.valueOf(encoder.decodeFloat(val));
		      } else if (clazz.equals(Double.TYPE) || clazz.equals(Double.class)) {
		          return (K) Double.valueOf(encoder.decodeDouble(val));
		      } else if (clazz.equals(String.class)) {
		          return (K) new String(val, "UTF-8");
		      } else if (clazz.equals(Utf8.class)) {
		          return (K) new Utf8(val);
		      }
		      
		      throw new IllegalArgumentException("Unknown type " + clazz.getName());
		      } catch (IOException ioe) {
		          throw new RuntimeException(ioe);
		    }
      }
	  
	  private static byte[] copyIfNeeded(byte b[], int offset, int len) {
		    if (len != b.length || offset != 0) {
		      byte copy[] = new byte[len];
		      System.arraycopy(b, offset, copy, 0, copy.length);
		      b = copy;
		    }
		    return b;
	  }
	  
	  public byte[] toBytes(Object o) {
		    return toBytes(encoder, o);
	  }
	  
	  public static byte[] toBytes(Encoder encoder, Object o) {
		    
		    try {
		      if (o instanceof String) {
		        return ((String) o).getBytes("UTF-8");
		      } else if (o instanceof Utf8) {
		        return copyIfNeeded(((Utf8) o).getBytes(), 0, ((Utf8) o).getLength());
		      } else if (o instanceof ByteBuffer) {
		        return copyIfNeeded(((ByteBuffer) o).array(), ((ByteBuffer) o).arrayOffset() + ((ByteBuffer) o).position(), ((ByteBuffer) o).remaining());
		      } else if (o instanceof Long) {
		        return encoder.encodeLong((Long) o);
		      } else if (o instanceof Integer) {
		        return encoder.encodeInt((Integer) o);
		      } else if (o instanceof Short) {
		        return encoder.encodeShort((Short) o);
		      } else if (o instanceof Byte) {
		        return encoder.encodeByte((Byte) o);
		      } else if (o instanceof Boolean) {
		        return encoder.encodeBoolean((Boolean) o);
		      } else if (o instanceof Float) {
		        return encoder.encodeFloat((Float) o);
		      } else if (o instanceof Double) {
		        return encoder.encodeDouble((Double) o);
		      } else if (o instanceof Enum) {
		        return encoder.encodeInt(((Enum<?>) o).ordinal());
		      }
		    } catch (IOException ioe) {
		      throw new RuntimeException(ioe);
		    }
		    
		    throw new IllegalArgumentException("Uknown type " + o.getClass().getName());
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
		
		try {
			map.put(mapping.getMajorKey(),getSchemaName());
			LOG.debug("Schema: "+mapping.getMajorKey()+" was created successfully");
		} catch (Exception e) {
			LOG.error( e.getMessage(), e.getStackTrace().toString() );
		}
    }

	public boolean delete(K arg0) {
		
		return false;
	}

	public long deleteByQuery(Query<K, T> arg0) {
		
		return 0;
	}

	public void deleteSchema() {
		if (!schemaExists()){
		      return;
		}
		try {
			
		} catch (Exception e) {
			// TODO: handle exception
		}
		map.remove(mapping.getMajorKey());
		
		
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
