package com.taobao.timetunnel.client;

import java.io.File;
import java.lang.reflect.Array;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.taobao.timetunnel.client.BufferFinance.Accountant;
import com.taobao.timetunnel.client.BufferFinance.Cashier;
import com.taobao.timetunnel.zookeeper.ZooKeeperServerForTest;
import com.taobao.util.Race;

/**
 * {@link TestClient}
 * 
 * @author <a href=mailto:jushi@taobao.com>jushi</a>
 * @created 2010-12-2
 * 
 */
public final class TestClient {

  private TestClient() {}

  public static ClientFactory clientFactory(final String host, final int port) {
    return new ClientFactory(host, port);
  }

  public static BufferFinance finance(final int size, final int capacity) {
    return new BufferFinance(size, capacity);
  }

  public static void main(final String[] args) throws Exception {

    try {
      Command.valueOf(args[0]).startWith(args);
    } catch (final Exception e) {
      e.printStackTrace();
      printUsage(args);
    }
  }

  public static TestRuntimeReport newTestRuntimeReport(final long standard, final int printPeriod) {
    return new RealTimeTestRunTimeReport(standard, printPeriod);
  }

  public static TestRuntimeReport noneTestRuntimeReport() {
    return noneTestRuntimeReport;
  }

  public static void parallelPubs(final ClientFactory factory,
                                  final String tokens,
                                  final String category,
                                  final TestRuntimeReport report,
                                  final BufferFinance finance) throws Exception {
    Race.run(callables(factory, tokens, category, report, finance, pubFactory));
  }

  @SuppressWarnings("unchecked")
  public static void parallelPubsAndSubs(final ClientFactory factory,
                                         final String pubToken,
                                         final String subToken,
                                         final String category,
                                         final TestRuntimeReport pubReport,
                                         final TestRuntimeReport subReport,
                                         final BufferFinance finance) throws Exception {
    final Callable<Void>[] pubs =
      callables(factory, pubToken, category, pubReport, finance, pubFactory);
    final Callable<Void>[] subs =
      callables(factory, subToken, category, subReport, finance, subFactory);
    final List<Callable<Void>> list = new LinkedList<Callable<Void>>();
    for (final Callable<Void> callable : pubs) {
      list.add(callable);
    }
    for (final Callable<Void> callable : subs) {
      list.add(callable);
    }

    Race.run(list.toArray((Callable<Void>[]) Array.newInstance(Callable.class, 0)));
  }

  public static void parallelSubs(final ClientFactory factory,
                                  final String tokens,
                                  final String category,
                                  final TestRuntimeReport report,
                                  final BufferFinance finance) throws Exception {
    Race.run(callables(factory, tokens, category, report, finance, subFactory));
  }

  public static void retry(final int retry) {
    TestClient.retry = retry;
  }

  private static <V> Callable<V>[] callables(final ClientFactory factory,
                                             final String tokens,
                                             final String category,
                                             final TestRuntimeReport report,
                                             final BufferFinance finance,
                                             final CallableFactory<V> callableFactory) {
    final Matcher matcher = tokenPattern.matcher(tokens);
    if (!matcher.matches()) throw new IllegalArgumentException(tokens);
    final String tokenPrefix = matcher.group(1);
    final int begin = Integer.parseInt(matcher.group(2));
    final int end = Integer.parseInt(matcher.group(3));
    @SuppressWarnings("unchecked") final Callable<V>[] callables =
      (Callable<V>[]) Array.newInstance(Callable.class, end - begin);
    for (int i = begin; i < end; i++) {
      final String token = tokenPrefix + i;
      System.out.println(token);
      callables[i - begin] = callableFactory.createBy(factory, token, category, report, finance);
    }
    return callables;
  }

  private static void printUsage(final String[] args) {
    System.out.println("Invaild command and arguments : " + Arrays.toString(args));
    System.out.println("Usage : ");
    System.out.println("\tzookeeper  <server-port> <data-dir> <tick-time> <initialized-node-script-file>");
    System.out.println("\tzombie     <broker-host> <broker-port> <num>");
    System.out.println("\tpublisher  <tokens|pub[0-99]> <broker-host> <broker-port> <category> <message-size> <report-standard> <report-print-period> <times>");
    System.out.println("\tsubscriber <tokens|sub[0-99]> <broker-host> <broker-port> <category> <message-size> <report-standard> <report-print-period> <times>");
  }

  private static Callable<Void> pub(final ClientFactory factory,
                                    final String token,
                                    final TestRuntimeReport report,
                                    final String category,
                                    final Cashier cashier) {
    return new Pub(factory, token, retry, report, category, cashier);
  }

  private static Callable<Void> sub(final ClientFactory factory,
                                    final String token,
                                    final String category,
                                    final TestRuntimeReport report,
                                    final Accountant accountant) {
    return new Sub(factory, token, retry, report, category, accountant);
  }

  private final static CallableFactory<Void> pubFactory = new CallableFactory<Void>() {

    @Override
    public Callable<Void> createBy(final ClientFactory factory,
                                   final String token,
                                   final String category,
                                   final TestRuntimeReport report,
                                   final BufferFinance finance) {
      return pub(factory, token, report, category, finance.cashier());
    }

  };

  private final static CallableFactory<Void> subFactory = new CallableFactory<Void>() {

    @Override
    public Callable<Void> createBy(final ClientFactory factory,
                                   final String token,
                                   final String category,
                                   final TestRuntimeReport report,
                                   final BufferFinance finance) {
      return sub(factory, token, category, report, finance.accountant());
    }
  };

  private final static Pattern tokenPattern = Pattern.compile("([\\w/]+)\\[(\\d+)-(\\d+)\\]");

  private static final NoneTestRuntimeReport noneTestRuntimeReport = new NoneTestRuntimeReport();

  private static int retry = 0;

  /**
   * {@link Command}
   */
  public enum Command {
    zookeeper {
      @Override
      void startWith(final String... args) throws Exception {
        final int port = Integer.parseInt(args[1]);
        final String dataDir = args[2];
        final int tickTime = Integer.parseInt(args[3]);
        final File scriptFile = new File(args[4]);

        final ZooKeeperServerForTest zkServer = new ZooKeeperServerForTest(port, dataDir, tickTime);

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

          @Override
          public void run() {
            zkServer.shutdown();
          }
        }, "shutdown-zookeeper-server"));

        final Thread thread = new Thread(new Runnable() {

          @Override
          public void run() {
            try {
              zkServer.startup();
            } catch (final Exception e) {
              e.printStackTrace();
              System.exit(-1);
            }
          }
        }, "zookeeper-server");
        thread.start();

        Thread.sleep(200L); // wait for server started.
        new ZookeeperNodeCreater(("localhost:" + port), (tickTime * 2)).createNodesBy(scriptFile);

        thread.join();
      }
    },
    publisher {
      @Override
      void startWith(final String... args) throws Exception {
        new Driver(args) {

          @Override
          protected void doRun() throws Exception {
            parallelPubs(factory, tokens, category, report, finance);
          }
        }.call();

      }
    },
    subscriber {
      @Override
      void startWith(final String... args) throws Exception {
        new Driver(args) {

          @Override
          protected void doRun() throws Exception {
            parallelSubs(factory, tokens, category, report, finance);
          }
        }.call();

      }
    },
    zombie {
      @Override
      void startWith(final String... args) throws Exception {
        final String host = args[1];
        final int port = Integer.parseInt(args[2]);
        final int num = Integer.parseInt(args[3]);
        final List<Socket> deads = new ArrayList<Socket>();
        for (int i = 0; i < num; i++) {
          deads.add(new Socket(host, port));
        }

        final AtomicBoolean stop = new AtomicBoolean();
        final Runnable target = new Runnable() {

          @Override
          public void run() {
            for (final Socket socket : deads) {
              try {
                socket.close();
              } catch (final Exception e) {}
            }
            stop.set(true);
          }
        };
        Runtime.getRuntime().addShutdownHook(new Thread(target, "shutdown-zombie"));
        while (!stop.get()) {
          Thread.sleep(1000L);
        }
      }
    };

    abstract void startWith(String... args) throws Exception;

  }

  private interface CallableFactory<V> {
    Callable<V> createBy(final ClientFactory factory,
                         final String token,
                         final String category,
                         final TestRuntimeReport report,
                         final BufferFinance finance);
  }

  /**
   * {@link Driver}
   */
  private static abstract class Driver implements Callable<Void> {
    public Driver(final String... args) throws Exception {
      tokens = args[1];
      category = args[4];
      final String host = args[2];
      final int port = Integer.parseInt(args[3]);
      factory = clientFactory(host, port);

      final long standard = Long.parseLong(args[6]);
      final int printPeriod = Integer.parseInt(args[7]);
      report = newTestRuntimeReport(standard, printPeriod);

      final int size = Integer.parseInt(args[5]);
      final int capacity = Integer.parseInt(args[8]);
      finance = finance(size, capacity);
    }

    @Override
    public final Void call() throws Exception {
      doRun();
      System.out.println(report);
      return null;
    }

    protected abstract void doRun() throws Exception;

    protected final ClientFactory factory;
    protected final TestRuntimeReport report;
    protected final String category;
    protected final String tokens;
    protected final BufferFinance finance;

  }

  /**
   * {@link NoneTestRuntimeReport}
   */
  private static final class NoneTestRuntimeReport implements TestRuntimeReport {
    @Override
    public Counter counterOf(final Exception e) {
      return counter;
    }

    @Override
    public Counter counterOfMessage() {
      return counter;
    }

    @Override
    public Counter counterOfSuccess() {
      return counter;
    }

    @Override
    public void hit(final long elaspe) {}

    private final Counter counter = new NoneCounter();

    private static final class NoneCounter implements Counter {

      @Override
      public void add(final int size) {}

      @Override
      public void increment() {}

    }
  }

}
