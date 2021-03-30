package com.taobao.timetunnel.center;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.File;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import com.taobao.timetunnel.InjectMocksSupport;
import com.taobao.timetunnel.InvalidTokenException;
import com.taobao.timetunnel.center.Center.ClusterChangedWatcher;
import com.taobao.timetunnel.client.ZookeeperNodeCreater;
import com.taobao.timetunnel.message.Category;
import com.taobao.timetunnel.session.Session;
import com.taobao.timetunnel.zookeeper.ZooKeeperConnector;
import com.taobao.timetunnel.zookeeper.ZooKeeperConnector.ZooKeeperListener;
import com.taobao.timetunnel.zookeeper.ZooKeeperServerForTest;
import com.taobao.util.Bytes;
import com.taobao.util.DirectoryCleaner;

/**
 * {@link CenterTest}
 * 
 * @author <a href=mailto:jushi@taobao.com>jushi</a>
 * @created 2010-12-30
 * 
 */
public class CenterTest extends InjectMocksSupport {

  @Before
  public void setUp() throws Exception {
    final String dir = "target/ctzk";
    DirectoryCleaner.clean(dir);
    new File(dir).mkdirs();
    zks = new ZooKeeperServerForTest(12345, dir, 1000);
    zks.startup();
    Thread.sleep(100L);

    final File file = new File("src/test/resources/ctzk");
    new ZookeeperNodeCreater(connectString, sessionTimeout).createNodesBy(file);
    center = new ZookeeperCenter(connectString, sessionTimeout);
    center.register("info", "group", watcher);
  }

  @Test
  public void shouldTimeoutSessionAndRebalance() throws Exception {
    Thread.sleep(1000L);
    final long begin = System.currentTimeMillis();
    final Session pub1 = center.checkedSession(Bytes.toBuffer("/clients/pub1"));
    final Session sub1 = center.checkedSession(Bytes.toBuffer("/clients/sub1"));
    final long end = System.currentTimeMillis();
    Thread.sleep(2000L + (end - begin));
    try {
      assertThat(pub1.isInvalid(), is(true));
      center.checkedSession(Bytes.toBuffer("/clients/pub1"));
      fail("pub1 should timeout after 1 second.");
    } catch (final InvalidTokenException e) {}
    try {
      assertThat(sub1.isInvalid(), is(true));
      center.checkedSession(Bytes.toBuffer("/clients/sub1"));
      fail("sub1 should timeout after 1 second.");
    } catch (final InvalidTokenException e) {}

    final Session pub10 = center.checkedSession(Bytes.toBuffer("/clients/pub10"));
    final Session sub10 = center.checkedSession(Bytes.toBuffer("/clients/sub10"));
    final ZooKeeperConnector connector =
      new ZooKeeperConnector(connectString, sessionTimeout, listener);
    connector.connect();
    connector.create("/brokers/group/b", Bytes.NULL, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    Thread.sleep(100L);
    try {
      assertThat(pub10.isInvalid(), is(true));
      center.checkedSession(Bytes.toBuffer("/clients/pub10"));
      fail("pub10 should timeout after rebalance.");
    } catch (final InvalidTokenException e) {}
    try {
      assertThat(sub10.isInvalid(), is(true));
      center.checkedSession(Bytes.toBuffer("/clients/sub10"));
      fail("sub10 should timeout after rebalance.");
    } catch (final InvalidTokenException e) {}
    connector.disconnect();
  }

  @Test
  public void shouldAwareSessionTokenDeleted() throws Exception {
    Thread.sleep(1000L);
    Session session = center.checkedSession(Bytes.toBuffer("/clients/pub10"));
    final ZooKeeperConnector connector =
      new ZooKeeperConnector(connectString, sessionTimeout, listener);
    connector.connect();
    connector.delete("/clients/pub10", -1);
    connector.disconnect();
    assertThat(session.isInvalid(), is(true));
  }

  @Test
  public void shouldAwareSubscribersAdd() throws Exception {

    Thread.sleep(1000L);
    try {
      center.category("new");
      fail();
    } catch (Exception e) {}
    final ZooKeeperConnector connector =
      new ZooKeeperConnector(connectString, sessionTimeout, listener);
    connector.connect();
    connector.create("/categories/new/subscribers/ad",
                     Bytes.NULL,
                     Ids.OPEN_ACL_UNSAFE,
                     CreateMode.PERSISTENT);
    connector.disconnect();
    Thread.sleep(100L);
    center.category("new");

  }

  @Test
  public void shouldUpdateSubscribersOfCategory() throws Exception {
    final Category category = center.category("chat");
    assertThat(category.isInvaildSubscriber("dw"), is(false));
    assertThat(category.isInvaildSubscriber("ad"), is(true));
    final ZooKeeperConnector connector =
      new ZooKeeperConnector(connectString, sessionTimeout, listener);
    connector.connect();
    connector.create("/categories/chat/subscribers/ad",
                     Bytes.NULL,
                     Ids.OPEN_ACL_UNSAFE,
                     CreateMode.PERSISTENT);
    connector.disconnect();
    Thread.sleep(100L);
    assertThat(category.isInvaildSubscriber("ad"), is(false));
    assertThat(category.isInvaildSubscriber("dw"), is(false));
  }

  @After
  public void tearDown() throws Exception {
    center.unregister();
    try {
      zks.shutdown();
    } catch (final Exception e) {}
  }

  @Mock
  private ZooKeeperListener listener;

  private ZooKeeperServerForTest zks;

  private Center center;
  private final int sessionTimeout = 2000;
  private final String connectString = "localhost:12345";
  @Mock
  private ClusterChangedWatcher watcher;
}
