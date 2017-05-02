package org.apache.gossip.transport.http;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.gossip.manager.GossipCore;
import org.apache.gossip.manager.GossipManager;
import org.apache.gossip.model.Base;
import org.apache.gossip.transport.AbstractTransportManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.Base64;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class SimpleNettyTransportManager extends AbstractTransportManager {
  public static final Logger LOGGER = Logger.getLogger(GossipManager.class);
  
  private final HttpServer server = new HttpServer();
  private final int endpointPort;
  private final BlockingQueue<byte[]> messages = new ArrayBlockingQueue<byte[]>(1024);
  private final Thread endpointConsumer;
  private boolean requestEndpointShutdown = false;
  
  public SimpleNettyTransportManager(GossipManager gossipManager, GossipCore gossipCore) {
    super(gossipManager, gossipCore);
    this.endpointPort = gossipManager.getMyself().getUri().getPort();
    endpointConsumer = new Thread("test-endpoint-consumer") {
      public void run() {
        while (!requestEndpointShutdown) {
          try {
            byte[] buf = gossipManager.getTransportManager().read();
            try {
              Base message = gossipManager.getProtocolManager().read(buf);
              gossipCore.receive(message);
              gossipManager.getMemberStateRefresher().run();
            }
            catch (RuntimeException ex) {
              LOGGER.error("Unable to process message", ex);
            }
          } catch (IOException e) {
            // InterruptedException are completely normal here because of the blocking lifecycle.
            if (!(e.getCause() instanceof InterruptedException)) {
              LOGGER.error(e);
            }
          }
        }
      }
    };
  }

  @Override
  public void startEndpoint() {
    endpointConsumer.start();
    server.start();  
  }

  @Override
  public void send(URI endpoint, byte[] buf) throws IOException {
    String base64Msg = Base64.getEncoder().encodeToString(buf);
    new HttpClient(base64Msg)
        .send(endpoint.getHost(), endpoint.getPort());
  }

  @Override
  public byte[] read() throws IOException {
    try {
      byte[] msg = messages.take();
      return msg;
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public void shutdown() {
    requestEndpointShutdown = true;
    endpointConsumer.interrupt();
    server.shutdown();
    super.shutdown();
  }

  private class HttpClient {
    private String message;
    HttpClient(String message) {
      this.message = message;
    }
    
    public void send(String host, int port) {
      EventLoopGroup group = new NioEventLoopGroup();
      try {
        Bootstrap b = new Bootstrap();
        b.group(group)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .handler(new ChannelInitializer<SocketChannel>() {
              @Override
              protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline cp = ch.pipeline();
                cp.addLast(new HttpClientCodec());
                //cp.addLast(new HttpContentDecompressor());
                //cp.addLast(new HttpObjectAggregator());
                cp.addLast(new SimpleChannelInboundHandler<HttpObject>() {
                  @Override
                  protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
                    if (msg instanceof HttpResponse) {
                      HttpResponse res = (HttpResponse)msg; 
//                      LOGGER.info("RESPONSE: " + res.getStatus());
                    } else if (msg instanceof HttpContent) {
                      HttpContent res = (HttpContent)msg;
//                      LOGGER.info("CONTENT: " + res.content().toString());
                      if (res instanceof LastHttpContent) {
                        ctx.close();
                      }
                    }
                  }
                });
              }
            });
        try {
          Channel ch = b.connect(host, port).sync().channel();
          HttpRequest req  = new DefaultFullHttpRequest(
              HttpVersion.HTTP_1_1, HttpMethod.GET, String.format("http://%s:%d/gossip", host, port));
          req.headers().set(HttpHeaderNames.HOST, host);
          req.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
          //req.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
          //req.headers().set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.GZIP);
          req.headers().set("zzz-gossip-message", message);
          ch.writeAndFlush(req);
          ch.closeFuture().sync();
        } catch (InterruptedException ex) {
          // todo: not sure what to do.
          LOGGER.warn(ex.getMessage(), ex);
        } catch (Throwable th) {
          LOGGER.error(th.getMessage(), th);
        }
      } finally {
        group.shutdownGracefully();
      }
    }
  }
  
  private class HttpServer {
    private Channel channel;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    
    HttpServer() {
      bossGroup = new NioEventLoopGroup(1);
      workerGroup = new NioEventLoopGroup();
    }
    
    void start() {
      Runtime.getRuntime().addShutdownHook(new Thread("http shutdown") {
        public void run() { shutdown(); }
      });
      
      final ServerBootstrap boostrap = new ServerBootstrap()
          .group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .handler(new LoggingHandler(LogLevel.TRACE))
          .childHandler(new ChannelInitializer<SocketChannel>() {
            
            @Override
            protected void initChannel(SocketChannel sc) throws Exception {
              sc.pipeline().addLast(new HttpRequestDecoder());
              //sc.pipeline().addLast(new HttpObjectAggregator(512 * 1024));
              sc.pipeline().addLast(new HttpResponseEncoder());
              //sc.pipeline().addLast(new HttpContentCompressor());
              sc.pipeline().addLast(new SimpleChannelInboundHandler() {
                private HttpRequest req;
                
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
                  if (msg instanceof HttpRequest) {
                    //LOGGER.info("SERVER REQUEST");
                    this.req = (HttpRequest) msg;
                    try {
                      String base64Msg = req.headers().get("zzz-gossip-message");
                      byte[] buf = Base64.getDecoder().decode(base64Msg);
                      messages.put(buf);
                    } catch (Exception ex) {
                      LOGGER.warn(ex.getMessage());
                    }
                  } else if (msg instanceof HttpContent) {
                    //LOGGER.info("SERVER CONTENT");
                    if (msg instanceof LastHttpContent) {
                      //LOGGER.info("SERVER LAST CONTENT");
                      FullHttpResponse res = new DefaultFullHttpResponse(
                          HttpVersion.HTTP_1_1,
                          HttpResponseStatus.OK,
                          Unpooled.buffer(0)
                      );
                      res.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain");
                      res.headers().set(HttpHeaders.Names.CONTENT_LENGTH, 0);
                      ctx.write(res);
                      if (!HttpUtil.isKeepAlive(req)) {
                        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
                      }
                    }
                  }
                }
              });
            }
          });
//          .option(ChannelOption.SO_BACKLOG, 128)
//          .option(ChannelOption.SO_KEEPALIVE, true);
      
      try {
        channel = boostrap.bind(endpointPort).sync().channel();
      } catch (InterruptedException ex) {
        // todo: what to do about this? shutdown?
        LOGGER.warn(ex.getMessage(), ex);
      }
    }
    
    void shutdown() {
      workerGroup.shutdownGracefully();
      bossGroup.shutdownGracefully();
      try {
        channel.closeFuture().sync();
      } catch (InterruptedException expected) {
        // just eat it.
        LOGGER.warn(expected.getMessage(), expected);
      }
    }
  }
}
