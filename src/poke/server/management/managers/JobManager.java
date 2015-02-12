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

import io.netty.channel.ChannelFuture;

import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.management.ManagementQueue;
import eye.Comm.JobBid;
import eye.Comm.JobProposal;
import eye.Comm.Management;
import eye.Comm.LeaderElection.VoteAction;

/**
 * The job manager class is used by the system to assess and vote on a job. This
 * is used to ensure leveling of the servers take into account the diversity of
 * the network.
 * 
 * @author gash
 * 
 */
public class JobManager {
	protected static Logger logger = LoggerFactory.getLogger("management");
	protected static AtomicReference<JobManager> instance = new AtomicReference<JobManager>();

	private String nodeId;

	public static JobManager getInstance(String id) {
		instance.compareAndSet(null, new JobManager(id));
		return instance.get();
	}

	public static JobManager getInstance() {
		return instance.get();
	}

	public JobManager(String nodeId) {
		this.nodeId = nodeId;
	}

	/**
	 * a new job proposal has been sent out that I need to evaluate if I can run
	 * it
	 * 
	 * @param req
	 *            The proposal
	 */
	/*
	 * preocessRequest(JobProposal)
	 * handle jobs it has received 
	 * ------------------------------
	 * 1) If this node is one of the leaders, it means this proposal is "competition"
	 *    -a. the leader would forward this proposal to all its slaves to get a bid(yes or no)
	 *    -b. the majority of bids would win the voting, and leader would send back this decision to originator
	 *    -c. step b would be handled in processRequest(JobBid)
	 * 2) otherwise, this is a slave   
	 *    -a. generate a random number(1 or 0)
	 *    -b. send back this number
	 *    
	 *  -- Shibai  
	 */
	public void processRequest(JobProposal req) {
		// if leader is not present, then block
		// a new leader election is on its way
		if (ElectionManager.getInstance().leaderDown()) {
			// block
		}
		if (ElectionManager.getInstance().isLeader()) {
			JobProposal.Builder jp = JobProposal.newBuilder();
			jp.setNameSpace(req.getNameSpace());
			jp.setOwnerId(req.getOwnerId());
			jp.setWeight(1); // weight is always 1, for now
			jp.setJobId(req.getJobId());
			
			Management.Builder mb = Management.newBuilder();
			mb.setJobPropose(jp.build());
			
			// get slaves info from heartmanager
			// should be revised to read in a config file
			for (HeartbeatData hd :  HeartbeatManager.getInstance().incomingHB.values()) {
				// forward job proposal 
				try {
					System.out.println("forwarding job proposal to slave");
					ChannelFuture channel = ManagementQueue.connect(hd.getHost(),hd.getMgmtport());
					if (channel == null) return;
					
					if (channel.isDone() && channel.isSuccess()) {
						channel.channel().writeAndFlush(mb.build());
						logger.info("flushing");
					}
					
					channel.channel().closeFuture();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}else {
			// send out job bid
			// this receiving node should evaluate job and the load balance of itself, then make a decision
			// at the time being, we do nothing but just rely yes or no
			Random random = new Random(); 
			int bid = random.nextInt(2); 
			JobBid.Builder jbid = JobBid.newBuilder();
			jbid.setJobId(req.getJobId());
			jbid.setOwnerId(req.getOwnerId()); //This is the Cluster ID
			jbid.setNameSpace(req.getNameSpace());
			jbid.setBid(bid);
			
			// reuse this channel, send bid back

		}
	}

	/**
	 * a job bid for my job
	 * 
	 * @param req
	 *            The bid
	 */
	
	/*
	 * processRequest(JobBid) 
	 * handles job bids that are sent by others
	 * ----------------------------------------
	 * Compare cluster id to current node's cluster id, 
	 * 1) if it is the same and the result is positive(yes),then this is the originator. 
	 *    Set the winner cluster and send it back to client.
	 * 	 -a. response message is in a form of Request.
	 *   -b. get its PerChannelQueue and call enqueueRepsonse(Request).
	 * 2) if not, check if all votes are in, then send the results back to originator
	 *   -a. run a loop to check all votes are in  
	 *   -b. if not, break
	 *   -c. if all in send it back
	 *   
	 *   * Implementation to be completed * 
	 *   
	 *   --Shibai
	 */
	public void processRequest(JobBid req) {
		// collect votes
		
		
	}
}
