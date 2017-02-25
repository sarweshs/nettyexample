package com.yuktix.server;


import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.slf4j.LoggerFactory;

public class TestOutputServer {


    public static void main(String[] args) throws Exception {
        TestOutputServer testServer = new TestOutputServer();
        testServer.run();
    }


    private void run() {
        ServerBootstrap bootstrap = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));

        // Configure the pipeline factory.
        bootstrap.setPipelineFactory(new TelnetServerPipelineFactory());

        // Bind and start to accept incoming connections.
        bootstrap.bind(new InetSocketAddress(4242));
    }


    private class TelnetServerPipelineFactory implements
            ChannelPipelineFactory {

        public ChannelPipeline getPipeline() throws Exception {
            // Create a default pipeline implementation.
            ChannelPipeline pipeline = Channels.pipeline();

            // Add the text line codec combination first,
            pipeline.addLast("framer", new DelimiterBasedFrameDecoder(
                    8192, Delimiters.lineDelimiter()));
            pipeline.addLast("decoder", new StringDecoder());
            pipeline.addLast("encoder", new StringEncoder());

            // and then business logic.
            pipeline.addLast("handler", new TelnetServerHandler());

            return pipeline;
        }
    }

    private class TelnetServerHandler extends SimpleChannelUpstreamHandler {

        private AtomicInteger numberOfReceived = new AtomicInteger();

        private final org.slf4j.Logger log = LoggerFactory.getLogger(TelnetServerHandler.class.getName());

        @Override
        public void handleUpstream(
                ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
            if (e instanceof ChannelStateEvent) {
                log.info(e.toString());
            }
            super.handleUpstream(ctx, e);
        }

        @Override
        public void channelConnected(
                ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
            // Send greeting for a new connection.
            e.getChannel().write(
                    "Welcome to " + InetAddress.getLocalHost().getHostName() + "!\r\n");
            e.getChannel().write("It is " + new Date() + " now.\r\n");
        }

        @Override
        public void messageReceived(
                ChannelHandlerContext ctx, MessageEvent e) {

            // Cast to a String first.
            // We know it is a String because we put some codec in TelnetPipelineFactory.
            String request = (String) e.getMessage();
            int numRec = numberOfReceived.incrementAndGet();


            // Generate and write a response.
            String response = null;
            boolean close = false;
            if (request.length() == 0) {
                response = "Please type something.\r\n";
            } else if (request.toLowerCase().equals("bye")) {
                response = "Have a good day!\r\n";
                close = true;
            } else {
                log.info(request);
                if (numRec % 100 == 1)
                    response = "Numrec: " + numRec;
            }

            // We do not need to write a ChannelBuffer here.
            // We know the encoder inserted at TelnetPipelineFactory will do the conversion.
            if (response != null || close) {
                ChannelFuture future = e.getChannel().write(response);

                // Close the connection after sending 'Have a good day!'
                // if the client has sent 'bye'.
                if (close) {
                    future.addListener(ChannelFutureListener.CLOSE);
                }
            }
        }

        @Override
        public void exceptionCaught(
                ChannelHandlerContext ctx, ExceptionEvent e) {
            log.error("Unexpected exception from downstream.", e);
            e.getChannel().close();
        }
    }


}
