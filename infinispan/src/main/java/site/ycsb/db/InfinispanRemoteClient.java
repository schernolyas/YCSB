/**
 * Copyright (c) 2015-2016 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License. See accompanying LICENSE file.
 */

package site.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

/**
 * This is a client implementation for Infinispan 5.x in client-server mode.
 */
public class InfinispanRemoteClient extends DB {

  private static final Log LOGGER = LogFactory.getLog(InfinispanRemoteClient.class);

  private RemoteCache<String, String> cache;

  @Override
  public void init() throws DBException {
    String cacheName = getProperties().getProperty("cache");
    cache = RemoteCacheManagerHolder.getInstance(getProperties()).getCache(cacheName);
  }

  @Override
  public void cleanup() {
  }

  @Override
  public Status insert(String table, String recordKey, Map<String, ByteIterator> values) {

    String compositKey = createKey(table, recordKey);
    Map<String, String> stringValues = new HashMap<>();
    StringByteIterator.putAllAsStrings(stringValues, values);
    try {
      cache.put(compositKey, "stringValues");
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.error(e);
      return Status.ERROR;
    }
  }

  @Override
  public Status read(String table, String recordKey, Set<String> fields, Map<String, ByteIterator> result) {

    String compositKey = createKey(table, recordKey);
    try {
      if (!cache.containsKey(compositKey)) {
        return Status.NOT_FOUND;
      }

      String value = cache.get(compositKey);

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.error(e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    LOGGER.warn("Infinispan does not support scan semantics");
    return Status.ERROR;
  }

  @Override
  public Status update(String table, String recordKey, Map<String, ByteIterator> values) {

    String compositKey = createKey(table, recordKey);
    try {
      Map<String, String> stringValues = new HashMap<>();
      StringByteIterator.putAllAsStrings(stringValues, values);
      cache.put(compositKey, "stringValues");
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.error(e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String recordKey) {

    String compositKey = createKey(table, recordKey);
    try {
      cache.remove(compositKey);
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.error(e);
      return Status.ERROR;
    }
  }


  private String createKey(String table, String recordKey) {
    return table + "-" + recordKey;
  }
}
