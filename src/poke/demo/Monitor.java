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
package poke.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.monitor.HeartMonitor;
import poke.monitor.MonitorListener;
import eye.Comm.Management;

/**
 * DEMO: how to listen to a heartbeat.
 * 
 * @author gash
 * 
 */
public class Monitor {
	protected static Logger logger = LoggerFactory.getLogger("client");

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String host = "localhost";
		int mport = 5670;

		if (args.length == 2) {
			try {
				host = args[0];
				mport = Integer.parseInt(args[1]);
			} catch (NumberFormatException e) {
				logger.warn("Unable to set port numbes, using default: 5670/5680");
			}
		}

		logger.info("trying to connect monitor to " + host + ":" + mport);
		HeartMonitor hm = new HeartMonitor("app", host, mport);
		hm.addListener(new HeartPrintListener(null));
		hm.waitForever();
	}

	public static class HeartPrintListener implements MonitorListener {
		protected static Logger logger = LoggerFactory.getLogger("client");

		// for filtering
		private String nodeID;

		/**
		 * create a listener of messages - demonstration
		 * 
		 * @param nodeID
		 *            IF not null, filter message by this node ID
		 */
		public HeartPrintListener(String nodeID) {
			this.nodeID = nodeID;
		}

		@Override
		public String getListenerID() {
			return nodeID;
		}

		@Override
		public void onMessage(Management msg) {
			if (logger.isDebugEnabled())
				logger.debug(msg.getBeat().getNodeId());

			// if the nodeID is set, we filter messages on it
			if (!nodeID.equals(msg.getBeat().getNodeId()))
				return;
			else if (msg.hasGraph()) {
				logger.info("Received graph responses from " + msg.getBeat().getNodeId());
			} else if (msg.hasBeat()) {
				logger.info("Received HB response: " + msg.getBeat().getNodeId());
			} else
				logger.error("Received management response from unexpected host: " + msg.getBeat().getNodeId());
		}

		@Override
		public void connectionClosed() {
			logger.error("Management port connection failed");
		}

		@Override
		public void connectionReady() {
			logger.info("Management port is ready to receive messages");
		}

	}

}
