package com.yuktix.netty;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NettyBeanstalkExample implements OutputConnection {
	
	public static void main(String[] args) throws Exception {
		NettyBeanstalkExample example = new NettyBeanstalkExample("192.168.0.14", 11300);
		example.connect();
		System.out.println("Got connection");
		example.destroy();
		System.out.println("Destroyed connection");
		
	}

    private static final Logger log = LoggerFactory.getLogger(NettyBeanstalkExample.class.getName());

    // Sleep 5 seconds before a reconnection attempt.
    static final int RECONNECT_DELAY = 5;
    //Error message handling
    private static final String CHANNEL_NOT_AVAILABLE = "Channel not initialized and is null.";
    private static final String CHANNEL_NOT_WRITABLE = "Channel is not writable.";

    private String beanstalkHost;
    private int beanstalkPort;
    private ClientBootstrap bootstrap;

    private volatile Channel activeChannel = null;

    public NettyBeanstalkExample(String beanstalkHost, int beanstalkPort) throws Exception {
        if (beanstalkHost == null)
            throw new Exception("Missing arg beanstalkHost");
        if (beanstalkPort == 0)
            throw new Exception("Illegal arg beanstalkPort (was 0)");

        this.beanstalkHost = beanstalkHost;
        this.beanstalkPort = beanstalkPort;

        // Make netty use SL4J as the logger
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());

        // Initialize the timer that schedules subsequent reconnection attempts.
        final Timer timer = new HashedWheelTimer();

        this.bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()));


        this.bootstrap.setPipelineFactory(new BeanstalkClientPipelineFactory(timer, this.bootstrap));
    }

    public synchronized void connect() throws Exception {
        if (activeChannel != null)
            throw new Exception("Can't connect since channel is already open");

        bootstrap.setOption("remoteAddress", new InetSocketAddress(this.beanstalkHost, this.beanstalkPort));
        // Start the connection attempt.
        ChannelFuture future = bootstrap.connect();

        // Wait until the connection attempt succeeds or fails.
        Channel channel = future.awaitUninterruptibly().getChannel();
        if (!future.isSuccess()) {
            bootstrap.releaseExternalResources();
            //noinspection ThrowableResultOfMethodCallIgnored
            throw new Exception("Error while connecting to beanstalk on " + this.beanstalkHost + ":" + this.beanstalkPort, future.getCause());
        }

        this.activeChannel = channel;
    }

    public synchronized void disconnect() throws Exception {
        if (activeChannel == null) {
            log.warn("Already disconnected");
            return;
        }
        activeChannel.close().awaitUninterruptibly();
        activeChannel = null;
        log.warn("Disconnected");
    }

    public synchronized void destroy() throws Exception {
        disconnect();
        this.bootstrap.releaseExternalResources();
    }

    public void sendToBeanstalkd(String s) throws Exception {
        if (activeChannel == null)
        {
            throw new Exception(CHANNEL_NOT_AVAILABLE);
        }
        if(!activeChannel.isWritable())
        {
            throw new Exception(CHANNEL_NOT_WRITABLE);
        }
        activeChannel.write(s + "\n");
    }

    public void outputMetric(String metric) throws Exception {
        sendToBeanstalkd(convertToBeanstalkdFormat(metric));
    }

    private String convertToBeanstalkdFormat(String metric) {
        if (metric == null)
            throw new NullPointerException("Metric was null");
        return "put " + metric;
    }


    private class BeanstalkdClientHandler extends SimpleChannelUpstreamHandler {

        private final Logger log = LoggerFactory.getLogger(BeanstalkdClientHandler.class.getName());

        private final ClientBootstrap bootstrap;
        private final Timer timer;

        private BeanstalkdClientHandler(ClientBootstrap bootstrap, Timer timer) {
            this.bootstrap = bootstrap;
            this.timer = timer;
        }

        private InetSocketAddress getRemoteAddress() {
            return (InetSocketAddress) this.bootstrap.getOption("remoteAddress");
        }

        @Override
        public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
            this.log.info("Disconnected from: " + getRemoteAddress());
        }

        @Override
        public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) {
            this.log.info("Sleeping for: " + RECONNECT_DELAY + "s");
            this.timer.newTimeout(new TimerTask() {
                public void run(Timeout timeout) throws Exception {
                    log.info("Reconnecting to: " + getRemoteAddress());
                    bootstrap.connect();
                }
            }, RECONNECT_DELAY, TimeUnit.SECONDS);
            activeChannel = null;
        }

        @Override
        public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
            this.log.info("Connected to: " + getRemoteAddress());
            activeChannel = e.getChannel();
        }


        @Override
        public void handleUpstream(
                ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
            if (e instanceof ChannelStateEvent) {
                this.log.info(e.toString());  // TOdo imple upstream handling of beanstalk responses. Like acting on errors.
            }
            super.handleUpstream(ctx, e);
        }

        @Override
        public void messageReceived(
                ChannelHandlerContext ctx, MessageEvent e) {
            // Print out the line received from the server.
            this.log.info(e.getMessage().toString());
        }

        @Override
        public void exceptionCaught(
                ChannelHandlerContext ctx, ExceptionEvent e) {
            //noinspection ThrowableResultOfMethodCallIgnored
            this.log.warn("Unexpected exception from downstream.",
                    e.getCause());
            e.getChannel().close();
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("NettyBeanstalkConnection");
        sb.append("{beanstalkHost='").append(beanstalkHost).append('\'');
        sb.append(", beanstalkPort=").append(beanstalkPort);
        sb.append('}');
        return sb.toString();
    }

    private class BeanstalkClientPipelineFactory implements ChannelPipelineFactory {

        private final Timer timer;
        private final ClientBootstrap bootstrap;

        private BeanstalkClientPipelineFactory(Timer timer, ClientBootstrap bootstrap) {
            this.timer = timer;
            this.bootstrap = bootstrap;
        }

        public ChannelPipeline getPipeline() throws Exception {
            // Create a default pipeline implementation.
            ChannelPipeline pipeline = Channels.pipeline();

            //pipeline.addLast("readTimeoutHandler", new ReadTimeoutHandler(timer, READ_TIMEOUT));
            // Todo: add something that pings beanstalk and always has a response?


            // Add the text line codec combination first,
            pipeline.addLast("framer", new DelimiterBasedFrameDecoder(
                    8192, Delimiters.lineDelimiter()));
            pipeline.addLast("decoder", new StringDecoder());
            pipeline.addLast("encoder", new StringEncoder());

            // and then business logic.
            pipeline.addLast("handler", new BeanstalkdClientHandler(this.bootstrap, this.timer));

            return pipeline;
        }
    }

}

