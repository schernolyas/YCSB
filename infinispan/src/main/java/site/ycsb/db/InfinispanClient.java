/**
 * Copyright (c) 2012-2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License. See accompanying LICENSE file.
 */

package site.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

/**
 * This is a client implementation for Infinispan 10.x.
 */
public class InfinispanClient extends DB {

  private static final Log LOGGER = LogFactory.getLog(InfinispanClient.class);

  // An optimisation for clustered mode
  private final boolean clustered = false;

  private EmbeddedCacheManager infinispanManager;


  public void init() throws DBException {
    infinispanManager = new DefaultCacheManager();
  }

  public void cleanup() {
    infinispanManager.stop();
    infinispanManager = null;
  }

  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      Map<String, String> row;
      Cache<String, Map<String, String>> cache = infinispanManager.getCache(table);
      row = cache.get(key);
      if (row != null) {
        result.clear();
        if (fields == null || fields.isEmpty()) {
          StringByteIterator.putAllAsByteIterators(result, row);
        } else {
          for (String field : fields) {
            result.put(field, new StringByteIterator(row.get(field)));
          }
        }
      }
      return Status.OK;
    } catch (Exception e) {
      LOGGER.error(e);
      return Status.ERROR;
    }
  }

  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    LOGGER.warn("Infinispan does not support scan semantics");
    return Status.OK;
  }

  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      Cache<String, Map<String, String>> cache = infinispanManager.getCache(table);
      Map<String, String> row = cache.get(key);
      if (row == null) {
        row = StringByteIterator.getStringMap(values);
        cache.put(key, row);
      } else {
        StringByteIterator.putAllAsStrings(row, values);
      }
      return Status.OK;
    } catch (Exception e) {
      LOGGER.error(e);
      return Status.ERROR;
    }
  }

  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      infinispanManager.getCache(table).put(key, values);
      return Status.OK;
    } catch (Exception e) {
      LOGGER.error(e);
      return Status.ERROR;
    }
  }

  public Status delete(String table, String key) {
    try {
      infinispanManager.getCache(table).remove(key);
      return Status.OK;
    } catch (Exception e) {
      LOGGER.error(e);
      return Status.ERROR;
    }
  }
}
