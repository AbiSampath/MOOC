
package poke.resources;

import static org.junit.Assert.*;

import org.junit.Test;

import poke.bean.Course;
import eye.Comm.Header;
import eye.Comm.Header.Routing;
import eye.Comm.JobOperation;
import eye.Comm.JobOperation.JobAction;
import eye.Comm.Payload;
import eye.Comm.Request;

/*
 * tests the listCourses method in JobResource
 * Abinaya Sampath
 */
public class JobResourceTest {
	
	private Request buildRequest() {
		Request.Builder req = Request.newBuilder();
		Header.Builder header=Header.newBuilder();
		header.setRoutingId(Routing.JOBS);
		header.setOriginator("Client");
		req.setHeader(header.build());	
		Payload.Builder body=Payload.newBuilder();
		JobOperation.Builder job_op=JobOperation.newBuilder();
		job_op.setAction(JobAction.LISTJOBS);
		body.setJobOp(job_op);
		req.setBody(body.build());
		return req.build();
	}
	
	@Test
	public void testListCourse() {
		JobResource job=new JobResource();
		Request req=buildRequest();
		Request res = job.listCourses(req);
		res.getBody().getJobStatus().getData(0).getOptions().getNode(0).getValue();
		assertEquals("result ","CMPE202",res.getBody().getJobStatus().getData(0).getOptions().getNode(0).getValue());
			
		}
/*	@Test
	public void testgetDescription() {
		JobResource job=new JobResource();
		Request req=buildRequest();
		Request res =job.getDescription(req);
		res.getBody().getJobStatus().getData(0).getOptions().getValue();
		assertEquals("result ","This is about software design",res.getBody().getJobStatus().getData(0).getOptions().getNode(0).getValue());
	}
*/		
		
	

}
