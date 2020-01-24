/**
 * Copyright (c) 2018 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db.ignite;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.log4j2.Log4J2Logger;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.measurements.Measurements;

/**
 * Integration tests for the Ignite client
 */
public class IgniteBatchClientTest extends IgniteClientTestBase {

  private final static String HOST = "127.0.0.1";
  private final static String PORTS = "47500..47509";
  private final static String SERVER_NODE_NAME = "YCSB Server Node";
  public static final int TOTAL_KEYS = 4;
  private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

  @BeforeClass
  public static void beforeTest() throws IgniteCheckedException {
    IgniteConfiguration igcfg = new IgniteConfiguration();
    igcfg.setIgniteInstanceName(SERVER_NODE_NAME);
    igcfg.setClientMode(false);

    TcpDiscoverySpi disco = new TcpDiscoverySpi();
    Collection<String> adders = new LinkedHashSet<>();
    adders.add(HOST + ":" + PORTS);

    ((TcpDiscoveryVmIpFinder) ipFinder).setAddresses(adders);
    disco.setIpFinder(ipFinder);

    igcfg.setDiscoverySpi(disco);
    igcfg.setNetworkTimeout(2000);

    CacheConfiguration ccfg = new CacheConfiguration().setName(DEFAULT_CACHE_NAME);

    igcfg.setCacheConfiguration(ccfg);

    Log4J2Logger logger = new Log4J2Logger(IgniteBatchClientTest.class.getClassLoader().getResource("log4j2.xml"));
    igcfg.setGridLogger(logger);

    cluster = Ignition.start(igcfg);
    cluster.active();
  }

  @Before
  public void setUp() throws Exception {
    Properties p = new Properties();
    p.setProperty("hosts", HOST);
    p.setProperty("ports", PORTS);
    p.setProperty("fetchsize", "2");
    Measurements.setProperties(p);

    client = new IgniteClient();
    client.setProperties(p);
    client.init();
  }

  @Test
  public void testInsert() throws Exception {
    cluster.cache(DEFAULT_CACHE_NAME).clear();
    final String key = "key";
    for (int index=0;index< TOTAL_KEYS;index++) {
      final Map<String, String> input = new HashMap<>();
      input.put("field0", "value1"+index);
      input.put("field1", "value2"+index);
      final Status status = client.insert(DEFAULT_CACHE_NAME, key+index, StringByteIterator.getByteIteratorMap(input));
      assertThat(status, is(Status.OK));
    }
    assertThat(cluster.cache(DEFAULT_CACHE_NAME).size(), is(TOTAL_KEYS));
  }

  @Test
  public void testDelete() throws Exception {
    cluster.cache(DEFAULT_CACHE_NAME).clear();
    final String key1 = "key1";
    final Map<String, String> input1 = new HashMap<>();
    input1.put("field0", "value1");
    input1.put("field1", "value2");
    final Status status1 = client.insert(DEFAULT_CACHE_NAME, key1, StringByteIterator.getByteIteratorMap(input1));
    assertThat(status1, is(Status.OK));
    assertThat(cluster.cache(DEFAULT_CACHE_NAME).size(), is(1));

    final String key2 = "key2";
    final Map<String, String> input2 = new HashMap<>();
    input2.put("field0", "value1");
    input2.put("field1", "value2");
    final Status status2 = client.insert(DEFAULT_CACHE_NAME, key2, StringByteIterator.getByteIteratorMap(input2));
    assertThat(status2, is(Status.OK));
    assertThat(cluster.cache(DEFAULT_CACHE_NAME).size(), is(2));

    final Status status3 = client.delete(DEFAULT_CACHE_NAME, key2);
    assertThat(status3, is(Status.OK));
    assertThat(cluster.cache(DEFAULT_CACHE_NAME).size(), is(1));
  }

  @Test
  public void testRead() throws Exception {
    cluster.cache(DEFAULT_CACHE_NAME).clear();
    final String key = "key";
    final Map<String, String> input = new HashMap<>();
    input.put("field0", "value1");
    input.put("field1", "value2A");
    input.put("field3", null);
    for (int index=0;index<TOTAL_KEYS;index++) {
      final Status sPut = client.insert(DEFAULT_CACHE_NAME, key+index, StringByteIterator.getByteIteratorMap(input));
      assertThat(sPut, is(Status.OK));
    }

    assertThat(cluster.cache(DEFAULT_CACHE_NAME).size(), is(TOTAL_KEYS));

    final Set<String> fld = new TreeSet<>();
    fld.add("field0");
    fld.add("field1");
    fld.add("field3");

    final HashMap<String, ByteIterator> result = new HashMap<>();
    final Status sGet1 = client.read(DEFAULT_CACHE_NAME, key+"0", fld, result);
    assertThat(sGet1, is(Status.BATCHED_OK));
    final Status sGet2 = client.read(DEFAULT_CACHE_NAME, key+"1", fld, result);
    assertThat(sGet2, is(Status.OK));

    final HashMap<String, String> strResult = new HashMap<String, String>();
    for (final Map.Entry<String, ByteIterator> e : result.entrySet()) {
      if (e.getValue() != null) {
        strResult.put(e.getKey(), e.getValue().toString());
      }
    }
    assertThat(strResult, hasEntry("field0", "value1"));
    assertThat(strResult, hasEntry("field1", "value2A"));
  }

  @Test
  public void testReadAllFields() throws Exception {
    cluster.cache(DEFAULT_CACHE_NAME).clear();
    final String key = "key";
    final Map<String, String> input = new HashMap<>();
    input.put("field0", "value1");
    input.put("field1", "value2A");
    input.put("field3", null);
    final Status sPut = client.insert(DEFAULT_CACHE_NAME, key+"0", StringByteIterator.getByteIteratorMap(input));
    assertThat(sPut, is(Status.OK));
    assertThat(cluster.cache(DEFAULT_CACHE_NAME).size(), is(1));

    final Set<String> fld = new TreeSet<>();

    final HashMap<String, ByteIterator> result1 = new HashMap<>();
    Status sGet1 = client.read(DEFAULT_CACHE_NAME, key+"1", fld, result1);
    final Status sGet = client.read(DEFAULT_CACHE_NAME, key+"0", fld, result1);
    assertThat(sGet, is(Status.OK));

    final HashMap<String, String> strResult = new HashMap<String, String>();
    for (final Map.Entry<String, ByteIterator> e : result1.entrySet()) {
      if (e.getValue() != null) {
        strResult.put(e.getKey(), e.getValue().toString());
      }
    }
    assertThat(strResult, hasEntry("field0", "value1"));
    assertThat(strResult, hasEntry("field1", "value2A"));
  }

  @Test
  public void testReadNotPresent() throws Exception {
    cluster.cache(DEFAULT_CACHE_NAME).clear();
    final String key = "key";
    final Map<String, String> input = new HashMap<>();
    input.put("field0", "value1");
    input.put("field1", "value2A");
    input.put("field3", null);
    final Status sPut = client.insert(DEFAULT_CACHE_NAME, key, StringByteIterator.getByteIteratorMap(input));
    assertThat(sPut, is(Status.OK));
    assertThat(cluster.cache(DEFAULT_CACHE_NAME).size(), is(1));

    final Set<String> fld = new TreeSet<>();

    final String newKey = "newKey";
    final HashMap<String, ByteIterator> result1 = new HashMap<>();
    client.read(DEFAULT_CACHE_NAME, newKey+"0", fld, result1);
    final Status sGet = client.read(DEFAULT_CACHE_NAME, newKey+"1", fld, result1);
    assertThat(sGet, is(Status.NOT_FOUND));

  }
}
