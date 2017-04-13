package org.apache.gossip.harness;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Solo {
  private static final AtomicInteger nextPort = new AtomicInteger(5000);
  private URI uri;
  private GossipSettings settings = new GossipSettings();

  private GossipManagerBuilder.ManagerBuilder builder = GossipManagerBuilder.newBuilder()
      .gossipSettings(settings);
  private Logger logger;
  private Thread thread;
  
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
    builder.gossipMembers(members);
    thread.start(); 
  }
  
  public URI uri() { return uri; }
  
  public void run() {
    GossipManager manager = builder
        .uri(uri)
        .id(uri.toString())
        .build();
    manager.init();
    
    // start gossiping
    while (true) {
      logger.debug("checking nodes");
      try { Thread.sleep(5000); } catch (Exception ex) { }

      for (LocalMember m : manager.getLiveMembers()) {
        logger.info(String.format("local %s", m.toString()));
      }
      for (LocalMember m : manager.getDeadMembers()) {
        logger.info(String.format("dead %s", m.toString()));
      }
    }  
  }
  
  private static URI makeURI(int port) {
    try {
      return new URI("udp://localhost:" + port);
    }
    catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }
  
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