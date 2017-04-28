package org.apache.gossip.examples;

import org.apache.gossip.GossipSettings;
import org.apache.gossip.LocalMember;
import org.apache.gossip.Member;
import org.apache.gossip.manager.GossipManager;
import org.apache.gossip.manager.GossipManagerBuilder;
import org.apache.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class Solo {
  private static final Object DUMP_LOCK = new Object();
  private URI uri;
  private GossipSettings settings = new GossipSettings();

  private GossipManagerBuilder.ManagerBuilder builder = GossipManagerBuilder.newBuilder()
      .gossipSettings(settings);
  private Logger logger;
  private Thread thread;
  private GossipManager manager;
  private boolean running = false;
  
  public Solo(String clusterId, int port) {
    uri = makeURI(port);
    logger = Logger.getLogger(Multi.class.getName() + "-" + uri.toString());
    builder.cluster(clusterId);
    thread = new Thread(uri.toString()) { public void run() {
      Solo.this.run();
    }};
  }
  
  public void start(List<Member> members) {
    logger.debug("will seed with " + members.stream().map(Member::toString).collect(Collectors.joining(",")));
    running = true;
    builder.gossipMembers(members);
    thread.start(); 
  }
  
  public void stop() {
    running = false;
    manager.shutdown();
  }
  
  public URI uri() { return uri; }
  
  public void run() {
    manager = builder
        .uri(uri)
        .id(uri.toString())
        .build();
    manager.init();
    
    int hash = 0;
    // start gossiping
    while (running) {
      logger.debug("checking nodes");
      try { Thread.sleep(5000); } catch (Exception ex) { }
      
      Map<String, String> states = new HashMap<>();
      
      for (LocalMember m : manager.getLiveMembers()) {
        states.put(m.getUri().toString(), String.format("  %s is UP", m.getUri().toString()));
      }
      for (LocalMember m : manager.getDeadMembers()) {
        states.put(m.getUri().toString(), String.format("  %s is DOWN", m.getUri().toString()));
      }
      SortedSet<String> sortedKeys = new TreeSet<>();
      sortedKeys.addAll(states.keySet());
      int newHash = states.values().stream().map(String::hashCode).reduce(0, (a, b) -> a ^ b);
      
      // I only want one node at a time dumping state.
      synchronized (DUMP_LOCK) {
        if (newHash != hash) {
          logger.info(String.format("%s thinks:", manager.getMyself().getUri()));
          sortedKeys.forEach(h -> logger.info(states.get(h)));
        } else {
          logger.info(String.format("%s thinks nothing changed", manager.getMyself().getUri()));
        }
      }
      hash = newHash;
    } 
    logger.info(String.format("%s will stop checking", manager.getMyself().getUri()));
  }
  
  private static URI makeURI(int port) {
    try {
      return new URI("udp://localhost:" + port);
    }
    catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }
  
  // params: $CLUSTER_NAME $LOCAL_PORT $SEED_PORT
  // e.g.: cluster_0 5003 5000
  // e.g.: cluster_0 5003 NONE
  public static void main(String args[]) {
    final Logger logger = Logger.getLogger(Solo.class);
    String clusterId = args[0];
    int port = Integer.parseInt(args[1]);
    String[] friendStrArr = "NONE".equals(args[2]) ? new String[] {} : args[2].split(",");
    List<Member> members = new ArrayList<>(friendStrArr.length);
    for (int i = 0; i < friendStrArr.length; i++) {
      try {
        URI uri = new URI("udp://localhost:" + friendStrArr[i]);
        logger.debug("Will connect to " + uri.toString());
        members.add(new Member(clusterId, uri, uri.toString(), 0, new HashMap<>()) {});
      } catch (Exception any) { }
    }
    
    Solo solo = new Solo(clusterId, port);
    solo.start(members);
    
  }
}