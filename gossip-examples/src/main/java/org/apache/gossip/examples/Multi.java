package org.apache.gossip.examples;

import org.apache.gossip.Member;
import org.apache.log4j.Logger;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

public class Multi {
  private static final String clusterId = "cluster-" + UUID.randomUUID().toString();
  public static final List<URI> globalUris = new ArrayList<URI>();
  private static Logger logger = Logger.getLogger(Multi.class);
  
  private static List<Member> members() {
    List<Member> members = new ArrayList<>(globalUris.size());
    for (URI uri : globalUris) {
      members.add(new Member(clusterId, uri, uri.toString(), 0, new HashMap<String, String>()) {});
    }
    return members;
  }
  
  private static <T> List<T> random(List<T> list) {
    if (list.size() == 0) return new ArrayList<>(0);
    Random rand = new Random(System.nanoTime());
    List<T> src = new ArrayList<>(list);
    int count = rand.nextInt(src.size());
    List<T> dest = new ArrayList<>(count);
    while (count-- > 0) {
      dest.add(src.remove(rand.nextInt(src.size())));
    }
    return dest;
  }
  
  public static Member asMember(URI uri) {
      return new Member(clusterId, uri, uri.toString(), 0, new HashMap<>()) {};
  }
  
  public static void main(String args[]) {
    Random rand = new Random(System.nanoTime());
    int count = Integer.parseInt(args[0]);
    Solo[] runners = new Solo[count];
    for (int i = 0; i < runners.length; i++) {
      try {
        globalUris.add(new URI("udp://localhost:" + (5000 + i)));
      } catch (Exception ex) {
        System.exit(-1);
      }
    }
    for (int i = 0; i < runners.length; i++) {
      logger.info("Creating " + i);
      runners[i] = new Solo(clusterId, 5000 + i);
      runners[i].start(random(globalUris.stream().map(Multi::asMember).collect(Collectors.toList())));
      try { Thread.sleep(rand.nextInt(10000) + 1); } catch (Exception ex) {}
    }
    
//    try { Thread.sleep(30000L); } catch (Exception ex) {}
//    int victim = rand.nextInt(runners.length);
//    logger.info("Killing " + runners[victim].uri());
//    runners[victim].stop();
  }
}