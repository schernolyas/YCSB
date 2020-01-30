/**
 * Copyright (c) 2013-2018 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License. See accompanying LICENSE file.
 * <p>
 */
package site.ycsb.db.ignite;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;


/**
 * Ignite client.
 * <p>
 * See {@code ignite/README.md} for details.
 */
public class IgniteClient extends IgniteAbstractClient {

  /**
   *
   */
  private static Logger log = LogManager.getLogger(IgniteClient.class);


  /**
   * Cached binary type.
   */
  private BinaryType binType = null;
  /**
   * Cached binary type's fields.
   */
  private final ConcurrentHashMap<String, BinaryField> fieldsCache = new ConcurrentHashMap<>();
  /**
   * The batch size for batched inserts. Set to >0 to use batching
   */
  public static final String BATCH_SIZE = "batchsize";

  /**
   * The JDBC fetch size hinted to the driver.
   */
  public static final String FETCH_SIZE = "fetchsize";
  private int batchSize;
  private int fetchSize;
  private volatile boolean initialized = false;
  private ThreadLocal<Set<String>> keySet = ThreadLocal.withInitial(() -> new HashSet<>());

  @Override
  public void init() throws DBException {

    this.batchSize = getIntProperty(getProperties(), BATCH_SIZE);
    this.fetchSize = getIntProperty(getProperties(), FETCH_SIZE);
    super.init();
    initialized = true;
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @SuppressWarnings("checkstyle:NoWhitespaceBefore")
  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    if (fetchSize > 0) {
      //read by key collection
      Set<String> currectKeyCollectionToGet = keySet.get();
      currectKeyCollectionToGet.add(key);
      log.debug("Thread :" + Thread.currentThread().getName() + ";  size of currectKeyCollectionToGet :"
          + currectKeyCollectionToGet.size());
      if (currectKeyCollectionToGet.size() == fetchSize) {
        Status resultStatus = Status.OK;
        //time to get
        Map<String, BinaryObject> fetchResult = cache.getAll(currectKeyCollectionToGet);
        log.debug("Thread :" + Thread.currentThread().getName() + ";  get keys from Ignite :"
            + fetchResult.size());
        currectKeyCollectionToGet.clear();
        if (!fetchResult.isEmpty()) {
          if (binType == null) {
            BinaryObject po = fetchResult.values().iterator().next();
            binType = po.type();
          }
          for (Iterator<BinaryObject> it = fetchResult.values().iterator(); it.hasNext();) {
            constractObject(table, key, fields, result, it.next());
          }
        } else {
          resultStatus = Status.NOT_FOUND;
        }
        return resultStatus;
      } else {
        return Status.BATCHED_OK;
      }

    } else {
      try {
        BinaryObject po = cache.get(key);
        if (po == null) {
          return Status.NOT_FOUND;
        }
        if (binType == null) {
          binType = po.type();
        }
        constractObject(table, key, fields, result, po);
        return Status.OK;
      } catch (Exception e) {
        log.error(String.format("Error reading key: %s", key), e);

        return Status.ERROR;
      }
    }
  }

  private void constractObject(String table, String key, Set<String> fields, Map<String, ByteIterator> result,
      BinaryObject po) {
    for (String fieldName : F.isEmpty(fields) ? binType.fieldNames() : fields) {
      BinaryField bfld = fieldsCache.get(fieldName);

      if (bfld == null) {
        bfld = binType.field(fieldName);
        fieldsCache.put(fieldName, bfld);
      }

      String val = bfld.value(po);
      if (val != null) {
        result.put(fieldName, new StringByteIterator(val));
      }

      if (debug) {
        log.info("table:{" + table + "}, key:{" + key + "}" + ", fields:{" + fields + "}");
        log.info("fields in po{" + binType.fieldNames() + "}");
        log.info("result {" + result + "}");
      }
    }
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key, overwriting any existing values with the same field name.
   *
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    try {
      cache.invoke(key, new Updater(values));

      return Status.OK;
    } catch (Exception e) {
      log.error(String.format("Error updating key: %s", key), e);

      return Status.ERROR;
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key.
   *
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    try {
      BinaryObjectBuilder bob = cluster.binary().builder("CustomType");

      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        bob.setField(entry.getKey(), entry.getValue().toString());

        if (debug) {
          log.info(entry.getKey() + ":" + entry.getValue());
        }
      }

      BinaryObject bo = bob.build();

      if (table.equals(DEFAULT_CACHE_NAME)) {
        cache.put(key, bo);
      } else {
        throw new UnsupportedOperationException("Unexpected table name: " + table);
      }

      return Status.OK;
    } catch (Exception e) {
      log.error(String.format("Error inserting key: %s", key), e);

      return Status.ERROR;
    }
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status delete(String table, String key) {
    try {
      cache.remove(key);
      return Status.OK;
    } catch (Exception e) {
      log.error(String.format("Error deleting key: %s ", key), e);
    }

    return Status.ERROR;
  }

  /**
   * Entry processor to update values.
   */
  public static class Updater implements CacheEntryProcessor<String, BinaryObject, Object> {

    private String[] flds;
    private String[] vals;

    /**
     * @param values Updated fields.
     */
    Updater(Map<String, ByteIterator> values) {
      flds = new String[values.size()];
      vals = new String[values.size()];

      int idx = 0;
      for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
        flds[idx] = e.getKey();
        vals[idx] = e.getValue().toString();
        ++idx;
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object process(MutableEntry<String, BinaryObject> mutableEntry, Object... objects)
        throws EntryProcessorException {
      BinaryObjectBuilder bob = mutableEntry.getValue().toBuilder();

      for (int i = 0; i < flds.length; ++i) {
        bob.setField(flds[i], vals[i]);
      }

      mutableEntry.setValue(bob.build());

      return null;
    }
  }

  /**
   * Returns parsed int value from the properties if set, otherwise returns -1.
   */
  private static int getIntProperty(Properties props, String key) throws DBException {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      try {
        return Integer.parseInt(valueStr);
      } catch (NumberFormatException nfe) {
        System.err.println("Invalid " + key + " specified: " + valueStr);
        throw new DBException(nfe);
      }
    }
    return -1;
  }
}
