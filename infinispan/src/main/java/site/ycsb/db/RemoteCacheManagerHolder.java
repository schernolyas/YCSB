/**
 * Copyright (c) 2015-2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db;

import java.util.Properties;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.commons.configuration.XMLStringConfiguration;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Utility class to ensure only a single RemoteCacheManager is created.
 */
final class RemoteCacheManagerHolder {
  private static final Log LOGGER = LogFactory.getLog(RemoteCacheManagerHolder.class);

  private static volatile RemoteCacheManager cacheManager = null;

  private RemoteCacheManagerHolder() {
  }

  static synchronized RemoteCacheManager getInstance(Properties props) {
    String ip = props.getProperty("ip");
    String user = props.getProperty("user");
    String password = props.getProperty("password");
    String cacheName = props.getProperty("cache");
    if (cacheManager == null) {
      LOGGER.info("=Create new instance of RemoteCacheManager=");
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host(ip).port(ConfigurationProperties.DEFAULT_HOTROD_PORT).tcpKeepAlive(true);
      builder.security().authentication()
          .saslMechanism("DIGEST-MD5")
          .username(user)
          .password(password);
      cacheManager = new RemoteCacheManager(builder.build(true));
      cacheManager.start();
      LOGGER.info("=cacheManager.getActiveConnectionCount() : "+cacheManager.getActiveConnectionCount()+"=");
      if (!cacheManager.getCacheNames().contains(cacheName)) {
        String xml = "<infinispan><cache-container><distributed-cache name=\"" + cacheName
            + "\"></distributed-cache></cache-container></infinispan>";
        cacheManager.administration().createCache(cacheName, new XMLStringConfiguration(xml));
      }

    }
    return cacheManager;
  }
}
