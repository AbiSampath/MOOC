/*
 * copyright 2014, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.monitor;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.management.ManagementInitializer;
import poke.server.management.ManagementQueue;
import eye.Comm.Management;
import eye.Comm.Network;
import eye.Comm.Network.NetworkAction;

/**
 * The monitor is a client-side component to process responses from server
 * management messages/responses - heartbeats (HB).
 * 
 * It is conceivable to create a separate application/process that listens to
 * the network. However, one must consider the HB (management port) is more for
 * localized communication through the overlay network and not as a tool for
 * overall health of the network. For an external monitoring device, a UDP-based
 * communication is more appropriate.
 * 
 * @author gash
 * 
 */
public class HeartMonitor {
	protected static Logger logger = LoggerFactory.getLogger("mgmt");

	protected ChannelFuture channel; // do not use directly, call connect()!
	private EventLoopGroup group;

	private static int N = 0;
	private String whoami;
	private String host;
	private int port;

	// this list is only used if the connection cannot be established - it holds
	// the listeners to be added.
	private List<MonitorListener> listeners = new ArrayList<MonitorListener>();

	private MonitorHandler handler;

	/**
	 * Create a heartbeat message processor.
	 * 
	 * @param host
	 *            the hostname
	 * @param port
	 *            This is the management port
	 */
	public HeartMonitor(String whoami, String host, int port) {
		this.whoami = whoami;
		this.host = host;
		this.port = port;
		this.group = new NioEventLoopGroup();
	}

	public MonitorHandler getHandler() {
		return handler;
	}

	/**
	 * abstraction of notification in the communication
	 * 
	 * @param listener
	 */
	public void addListener(MonitorListener listener) {
		if (handler == null && !listeners.contains(listener)) {
			listeners.add(listener);
			return;
		}

		try {
			handler.addListener(listener);
		} catch (Exception e) {
			logger.error("failed to add listener", e);
		}
	}

	public void release() {
		logger.warn("HeartMonitor releasing resources");

		for (String id : handler.listeners.keySet()) {
			MonitorListener ml = handler.listeners.get(id);
			ml.connectionClosed();

			// hold back listeners to re-apply if the connection is
			// re-established.
			listeners.add(ml);
		}

		// TODO should wait a fixed time and use a listener to reset values;s
		channel = null;
		handler = null;
	}

	/**
	 * create connection to remote server
	 * 
	 * @return
	 */
	protected Channel connect() {
		// Start the connection attempt.
		if (channel == null) {
			try {
				// handler = new MonitorHandler();
				// MonitorInitializer mi = new MonitorInitializer(handler, false);
				
				// use management queue to handle received heartbeat
				// - Shibai
				ManagementInitializer mi = new ManagementInitializer(false);

				// handler = new Managementhandler();
				
				Bootstrap b = new Bootstrap();
				// @TODO newFixedThreadPool(2);
				b.group(group).channel(NioSocketChannel.class).handler(mi);
				b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
				b.option(ChannelOption.TCP_NODELAY, true);
				b.option(ChannelOption.SO_KEEPALIVE, true);
				// Make the connection attempt.
				channel = b.connect(host, port).syncUninterruptibly();
				channel.awaitUninterruptibly(5000l);
				channel.channel().closeFuture().addListener(new MonitorClosedListener(this));
				
				if (N == Integer.MAX_VALUE)
					N = 1;
				else
					N++;

				// add listeners waiting to be added
				if (listeners.size() > 0) {
					for (MonitorListener ml : listeners)
						handler.addListener(ml);
					listeners.clear();
				}
			} catch (Exception ex) {
				logger.debug("failed to initialize the heartbeat connection");
				// logger.error("failed to initialize the heartbeat connection",
				// ex);
			}
		}

		if (channel != null && channel.isDone() && channel.isSuccess())
			return channel.channel();
		else
			throw new RuntimeException("Not able to establish connection to server");
	}

	public boolean isConnected() {
		if (channel == null)
			return false;
		else
			return channel.channel().isOpen();
	}

	public String getNodeInfo() {
		if (host != null)
			return host + ":" + port;
		else
			return "Unknown";
	}

	/**
	 * request the node to send heartbeats.
	 * 
	 * @return did a connect and message succeed
	 */
	public boolean startHeartbeat() {
		// the join will initiate the other node's heartbeatMgr to reply to
		// this node's (caller) listeners.

		boolean rtn = false;
		try {
			Channel ch = connect();

			logger.info("sending mgmt join message");
			Network.Builder n = Network.newBuilder();
			n.setNodeId("mgmt-" + whoami + "." + N);
			n.setAction(NetworkAction.NODEJOIN);
			Management.Builder m = Management.newBuilder();
			m.setGraph(n.build());
			// ManagementQueue.enqueueResponse(m.build(), ch);
			
			ch.writeAndFlush(m.build());
			rtn = true;
			logger.info("join message sent");
		} catch (Exception e) {
			// logger.error("could not send connect to node", e);
		}

		return rtn;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	/**
	 * for demo application only - this will enter a loop waiting for
	 * heartbeatMgr messages.
	 * 
	 * Notes:
	 * <ol>
	 * <li>this method is not used by the servers
	 * <li>blocks if connection is created otherwise, it returns if the node is
	 * not available.
	 * </ol>
	 */
	public void waitForever() {
		try {
			boolean connected = startHeartbeat();
			while (connected) {
				Thread.sleep(2000);
			}
			logger.info("---> trying to connect heartbeat");
		} catch (Exception e) {
			// e.printStackTrace();
		}
	}

	/**
	 * Called when the channel tied to the monitor closes. Usage:
	 * 
	 */
	public static class MonitorClosedListener implements ChannelFutureListener {
		private HeartMonitor monitor;

		public MonitorClosedListener(HeartMonitor monitor) {
			this.monitor = monitor;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			monitor.release();
		}
	}
}
