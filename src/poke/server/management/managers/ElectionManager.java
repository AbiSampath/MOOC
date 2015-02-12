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
package poke.server.management.managers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.management.ManagementQueue;
import poke.server.management.ManagementQueue.ManagementQueueEntry;
import eye.Comm.Heartbeat;
import eye.Comm.LeaderElection;
import eye.Comm.Management;
import eye.Comm.LeaderElection.VoteAction;

/**
 * The election manager is used to determine leadership within the network.
 * 
 * @author gash
 * 
 */
 /*
  * ElectionManager
  * using bully algorithm can detect leader failures and start a new leader election
  * Note:
  * 1) Currently ABSTAIN_VALUE works as an ack for election messages
  * 2) Topology information of the cluster is obtained from HeartBeatManager
  * 
  * -- Shibai
  * 
  */

public class ElectionManager extends Thread {
	protected static Logger logger = LoggerFactory.getLogger("management");
	protected static AtomicReference<ElectionManager> instance = new AtomicReference<ElectionManager>();

	private String nodeId;
	
	// Shibai
	private String leaderId;
	private boolean ack;
	private boolean myElection;
	private HeartbeatManager heartbeatMgr;

	/** @brief the number of votes this server can cast */
	private int votes = 1;

	public static ElectionManager getInstance(String id, int votes) {
		instance.compareAndSet(null, new ElectionManager(id, votes));
		return instance.get();
	}

	public static ElectionManager getInstance() {
		return instance.get();
	}

	public boolean isLeader () {
		return leaderId == nodeId;
	}
	
	public boolean leaderDown () {
		return leaderId == null;
	}

	
	/**
	 * initialize the manager for this server
	 * 
	 * @param nodeId
	 *            The server's (this) ID
	 */
	protected ElectionManager(String nodeId, int votes) {
		this.nodeId = nodeId;

		if (votes >= 0)
			this.votes = votes;

		heartbeatMgr = HeartbeatManager.getInstance();
		// broadCastNewElection();
		// declareElection();
	}
	
	/*
	 * Broadcast in network that a new election is coming
	 * - Shibai
	 */
	private void broadCastNewElection() {
		myElection = true;
		ack = false;
		leaderId = null;
		System.out.println(nodeId + ": delcare a new election");
		for (HeartbeatData hd : heartbeatMgr.incomingHB.values()) {
			sendRequest(hd, VoteAction.ELECTION,"New election!!");
		}
	}
	
	/*
	 * send declaration to all higher ids
	 * nominating
	 * - Shibai
	 */
	private void declareElection () {
		System.out.println(nodeId + ": nominating");
		for (HeartbeatData hd : heartbeatMgr.incomingHB.values()) {
			if (compIds(hd.getNodeId(), nodeId)) {
				System.out.println("sending nomination to: " + hd.getNodeId());
				sendRequest(hd, VoteAction.NOMINATE,"Nomination!");
			}
		}
	}
	
	/*
	 * send out request
	 * - Shibai
	 */
	private void sendRequest (HeartbeatData hd, eye.Comm.LeaderElection.VoteAction voteAction,String desc) {
		LeaderElection.Builder l = LeaderElection.newBuilder();
		l.setNodeId(nodeId);
		l.setBallotId("1");
		l.setVote(voteAction);
		l.setDesc(desc);

		Management.Builder m = Management.newBuilder();
		m.setElection(l.build());
		
		logger.info("sending request");
	
		try {
			ChannelFuture channel = ManagementQueue.connect(hd.getHost(),hd.getMgmtport());
			if (channel == null) return;
			
			if (channel.isDone() && channel.isSuccess()) {
				channel.channel().writeAndFlush(m.build());
				//ManagementQueue.enqueueResponse(m.build(), channel.channel());
				logger.info("flushing");
			}
			
			channel.channel().closeFuture();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/*
	 * compare node ids
	 * - Shibai
	 */
	private boolean compIds (String id1, String id2) {
		if (Integer.parseInt(id1) > Integer.parseInt(id2)) {
			return true;
		}
		return false;
	}
	
	/**
	 * - Shibai
	 * @param args
	 */
	public void processRequest(LeaderElection req) {
		if (req == null)
			return;

		if (req.hasExpires()) {
			long ct = System.currentTimeMillis();
			if (ct > req.getExpires()) {
				// election is over
				return;
			}
		}

		if (req.getVote().getNumber() == VoteAction.ELECTION_VALUE) {
			// an election is declared!
			System.out.println("received some one's election");
			myElection = false;
			leaderId = null;
			ack = false;
			// stop receiving new jobs
			
		} else if (req.getVote().getNumber() == VoteAction.DECLAREVOID_VALUE) {
			// no one was elected, I am dropping into standby mode`
			// left void
		} else if (req.getVote().getNumber() == VoteAction.DECLAREWINNER_VALUE) {
			// some node declared themself the leader
			// set leader
			System.out.println("some node declared itself the leader");
			leaderId = req.getNodeId();
			myElection = false;
			ack = false;
			System.out.println("ok, new leader is: " + leaderId);
		} else if (req.getVote().getNumber() == VoteAction.ABSTAIN_VALUE) {
			// for some reason, I decline to vote
			// work as an ack for now
			System.out.println("received ack, set flag to true");
			ack = true;
			
		} else if (req.getVote().getNumber() == VoteAction.NOMINATE_VALUE) {
			// send back acks if necessary 
			// send out request and set timeout 
			System.out.println("received nomination");
			if (!myElection) {
				System.out.println("sending back acks");
				myElection = true;
				sendAck(req);
				declareElection();
			}
			
			/* 
			 * commented out prof's code
			
			
			int comparedToMe = req.getNodeId().compareTo(nodeId);
			if (comparedToMe == -1) {
				// Someone else has a higher priority, forward nomination
				// TODO forward
				// left void. Since we are using bully algorithm, would never receive nominations from higher ids
			} else if (comparedToMe == 1) {
				// I have a higher priority, nominate myself
				// TODO nominate myself
				System.out.println("ready to have my election");
				if (!myElection) {
					myElection = true;
					sendAck(req);
					declareElection();
				}
			}
			*/
		} 
	}
	
	
	private void sendAck (LeaderElection req) {
		HeartbeatData hd = heartbeatMgr.incomingHB.get(req.getNodeId());
		System.out.println("send back acks");
		sendRequest(hd,VoteAction.ABSTAIN,"ACK"); // change ABSTAIN to ACK
	}
	
	
	
	/*
	 *  run()
	 *  each node would keep a sepertate thread running to check leader failure and ack reception
	 *  -----------------------------------------------------
	 *  1) first inner loop: 
	 *     check failure of leader, if failures are detected, start a new election,
	 *     in which it resets myelection to true, ack to false, leaderId to null.
	 *     first inner loop would be broken and move into second inner loop
	 *  2) second inner loop:   
	 *     -a. a new election is being held by current node. And it has send out nominations and waits for acks.
	 *     -b. if it receives any ack, it drops its own election and waits for some other node to declare as winner
	 *     -c. if no ack is received, it declare itself as winner and broadcast this msg across current cluster
	 *     
	 *  -- Shibai
	 *  
	 */
	@Override
	public void run () {
		broadCastNewElection();
		declareElection();
		
		while (true) {
			while (leaderId != nodeId && leaderId != null) {
				try {
					Thread.sleep(3000);
					HeartbeatData hd = heartbeatMgr.incomingHB.get(leaderId);
					if (leaderId != nodeId && leaderId != null && hd.getFailures() > 3) {
						broadCastNewElection();
						declareElection();
					}
				} catch (Exception e) {
					System.out.println("error in checking for failure");
					break;
				}
			}

			// during election, waiting for acks
			int failure = 0;
			while (leaderId == null && !ack) {
				try {
					Thread.sleep(3000);
					failure++;
					if (leaderId == null && !ack && failure > 5) {
						// no ack received, broadcast: I am the new leader!
						leaderId = nodeId;
						for (HeartbeatData hd : heartbeatMgr.incomingHB.values()) {
							sendRequest(hd, VoteAction.DECLAREWINNER,"I am the new leader!!");
						}
						System.out.println(nodeId + ": ok, im the new leader");
						break;
					}
				} catch (Exception e) {
					// e.printStackTrace();
					System.out.println("error in waiting for acks");
					break;
				}
			}
		}
	}
}
