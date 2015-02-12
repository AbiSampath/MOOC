/*
 * copyright 2012, gash
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
package poke.resources;

import io.netty.channel.ChannelFuture;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.hibernate.Query;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import persistence.HibernateUtil;
import poke.bean.Course;
import poke.bean.Student;
import poke.server.management.ManagementQueue;
import poke.server.management.managers.NetworkManager;
import poke.server.resources.Resource;
import poke.server.resources.ResourceUtil;
import eye.Comm.JobBid;
import eye.Comm.JobDesc;
import eye.Comm.JobDesc.JobCode;
import eye.Comm.JobProposal;
import eye.Comm.JobStatus;
import eye.Comm.Management;
import eye.Comm.NameValueSet;
import eye.Comm.NameValueSet.NodeType;
import eye.Comm.Payload;
import eye.Comm.PokeStatus;
import eye.Comm.Request;

public class JobResource implements Resource {
	protected static Logger logger = LoggerFactory.getLogger("server");
	protected String nameSpace;
	protected String courseName1;

	/*
	 * Processes the request from client
	 * Abinaya Sampath
	 */
	 
	@Override
	public Request process(Request request) {
		Request.Builder rb = Request.newBuilder();
		rb.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(),
				PokeStatus.SUCCESS, null));
		logger.info("poke"
				+ request.getBody().getJobOp().getData().getNameSpace());

		// get the nameSpace from the request
		nameSpace = request.getBody().getJobOp().getData().getNameSpace();

		
		//Calls the functions according to namespace
		
		if (nameSpace != null && nameSpace.equals("signup")) {
			Request reply = signUp(request);
			return reply;
		}
		if (nameSpace != null && nameSpace.equals("listcourses")) {
			Request reply = listCourses(request);
			return reply;
		}
		if (nameSpace != null && nameSpace.equals("getdescription")) {
			Request reply=getDescription(request);
			return reply;
		}
		if (nameSpace != null && nameSpace.equals("competition")) {
			Request reply=startCompetition(request);
			return reply;
		}
		Request reply = rb.build();
		return reply;
	}

	/*
	 * returns the description of a particular course
	 * Abinaya Sampath
	 */
	public Request getDescription(Request request) {
		courseName1 = request.getBody().getJobOp().getData().getOptions()
				.getValue();
		Session session = HibernateUtil.getSessionFactory().openSession();
		session.beginTransaction();
		
		//gets the description for courseName1
		Query query = session
				.createQuery("from Course where courseName=:courseName1");
		query.setParameter("courseName1", request.getBody().getJobOp()
				.getData().getOptions().getValue());
		List<Course> listCourse = query.list();
		Course course = (Course) listCourse.get(0);
		String courseDescription = course.getCourseDesc();
		session.getTransaction().commit();
		
		//sets the header for the reply
		Request.Builder rb = Request.newBuilder();
		rb.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(),
				PokeStatus.SUCCESS, null));
		
       //sets the body for the response
		Payload.Builder payLoad = Payload.newBuilder();
		JobStatus.Builder jobStatus = JobStatus.newBuilder();
		JobDesc.Builder jobDesc = JobDesc.newBuilder();
		NameValueSet.Builder nameValueSet = NameValueSet.newBuilder();
		nameValueSet.setNodeType(NodeType.VALUE);
		nameValueSet.setName("coursedescription");
		nameValueSet.setValue(courseDescription);
		nameValueSet.build();
		jobStatus.setJobId(request.getBody().getJobOp().getData().getJobId());
		jobDesc.setNameSpace("getDescription");
		jobDesc.setOwnerId(request.getBody().getJobOp().getData().getOwnerId());
		jobDesc.setJobId(request.getBody().getJobOp().getData().getJobId());
		jobDesc.setStatus(JobCode.JOBRECEIVED);
		jobDesc.setOptions(nameValueSet);
		jobDesc.build();
		jobStatus.setJobState(JobCode.JOBRECEIVED);
		jobStatus.setStatus(PokeStatus.SUCCESS);
		jobStatus.addData(jobDesc);
		payLoad.setJobStatus(jobStatus);
		payLoad.build();
		rb.setBody(payLoad);
		Request reply = rb.build();
		return reply;
	}

	/*
	 * returns the list of available courses
	 * Abinaya Sampath
	 */
	public Request listCourses(Request request) {
		System.out.println("Hibernate");
		Session session = HibernateUtil.getSessionFactory().openSession();
		session.beginTransaction();
		
		//Query to get the course from the database
		Query query = session.createQuery("from Course");
		List<Course> listCourse = query.list();
		System.out.println("Abinaya Sampath " + listCourse.toString());
		session.getTransaction().commit();
		//the courses are added to an arraylist
		List<NameValueSet> listNameValueSet = new ArrayList<NameValueSet>();

		for (Course course : listCourse) {
			listNameValueSet.add(buildNameValueSet(course));
		}

		NameValueSet.Builder nameValueSet = NameValueSet.newBuilder();
		nameValueSet.setName("coursename");
		nameValueSet.setNodeType(NodeType.NODE);
		nameValueSet.addAllNode(listNameValueSet);
		nameValueSet.build();
		//header is set for the response
		Request.Builder rb = Request.newBuilder();
		rb.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(),
				PokeStatus.SUCCESS, null));
		
		//body is set for the response
		Payload.Builder payLoad = Payload.newBuilder();
		JobStatus.Builder jobStatus = JobStatus.newBuilder();
		JobDesc.Builder jobDesc = JobDesc.newBuilder();
		jobStatus.setJobId(request.getBody().getJobOp().getData().getJobId());
		jobDesc.setNameSpace("listcourses");
		jobDesc.setOwnerId(request.getBody().getJobOp().getData().getOwnerId());
		jobDesc.setJobId(request.getBody().getJobOp().getData().getJobId());
		jobDesc.setStatus(JobCode.JOBRECEIVED);
		jobDesc.setOptions(nameValueSet);
		jobDesc.build();
		System.out.println("Abinaya namevalueset" + nameValueSet.toString());
		jobStatus.setJobState(JobCode.JOBRECEIVED);
		jobStatus.setStatus(PokeStatus.SUCCESS);
		jobStatus.addData(jobDesc);
		payLoad.setJobStatus(jobStatus);
		payLoad.build();
		rb.setBody(payLoad);
		Request reply = rb.build();
		return reply;

	}

	NameValueSet buildNameValueSet(Course course) {
		NameValueSet.Builder nameValueSet = NameValueSet.newBuilder();
		nameValueSet.setName("coursename");
		nameValueSet.setValue(course.getCourseName());
		nameValueSet.setNodeType(NodeType.VALUE);
		return nameValueSet.build();
	}

	/*
	 * adds the user data to the database
	 * Abinaya Sampath
	 */
	public Request signUp(Request request) {
		List<NameValueSet> studentData = request.getBody().getJobOp().getData()
				.getOptions().getNodeList();
		Session session = HibernateUtil.getSessionFactory().openSession();
		session.beginTransaction();
		Student student = new Student();
		System.out.println("Size :" + studentData.size());
		
		//gets the values from the request and sets it to the student object
		if (studentData.get(0).getName()!=null && studentData.get(0).getName().equals("email"))
			student.setEmail(studentData.get(0).getValue());
		if (studentData.get(1).getName()!=null && studentData.get(1).getName().equals("password"))
			student.setPassword(studentData.get(1).getValue());
		if (studentData.get(2).getName()!=null && studentData.get(2).getName().equals("fname"))
			student.setFname(studentData.get(2).getValue());
		if (studentData.get(3).getName()!=null && studentData.get(3).getName().equals("lname"))
			student.setLname(studentData.get(3).getValue());
		
		//student data is added to the table
		session.save(student);
		session.getTransaction().commit();
		
		//Header is set for the reply
		Request.Builder rb = Request.newBuilder();
		rb.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(),
				PokeStatus.SUCCESS, null));
		
		//Body is set for the reply
		Payload.Builder payLoad = Payload.newBuilder();
		JobStatus.Builder jobStatus = JobStatus.newBuilder();
		JobDesc.Builder jobDesc = JobDesc.newBuilder();
		jobStatus.setJobId(request.getBody().getJobOp().getData().getJobId());
		jobDesc.setNameSpace("Signup successful");
		jobDesc.setOwnerId(request.getBody().getJobOp().getData().getOwnerId());
		jobDesc.setJobId(request.getBody().getJobOp().getData().getJobId());
		jobDesc.setStatus(JobCode.JOBRECEIVED);
		NameValueSet.Builder nameValueSet=NameValueSet.newBuilder();
		nameValueSet.setName("Message");
		nameValueSet.setValue("User has beed added successfully");
		nameValueSet.setNodeType(NodeType.VALUE);
		jobDesc.setOptions(nameValueSet);
		jobStatus.setJobState(JobCode.JOBRECEIVED);
		jobStatus.setStatus(PokeStatus.SUCCESS);
		jobStatus.addData(jobDesc);
		payLoad.setJobStatus(jobStatus);
		payLoad.build();
		rb.setBody(payLoad);

		Request reply = rb.build();
		return reply;

	}
	
	/* Competition
	 * Global voting
	 * ---------------------------
	 * 1) build a message as in job proposal
	 * 2) retrieve the latest hashmap from global DNS, which contains cluster ids and leaders ips 
	 * 3) forward this job proposal to those leaders
	 *    a. create a new channel from ManagementQueue.connect() 		 
	 *    b. use JobManager to handle the responses
	 *    
	 *  * Implementation to be completed *
	 *  
	 * -- Shibai and Abinaya
	 */
	public Request startCompetition(Request request) {
		// form msg
		// Header is set for the reply
		Request.Builder rb = Request.newBuilder();
		rb.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(),
				PokeStatus.SUCCESS, null));

		// Body is set for the reply
		Payload.Builder payLoad = Payload.newBuilder();
		JobStatus.Builder jobStatus = JobStatus.newBuilder();
		JobDesc.Builder jobDesc = JobDesc.newBuilder();
		jobStatus.setJobId(request.getBody().getJobOp().getData().getJobId());
		jobDesc.setNameSpace("competition");
		jobDesc.setOwnerId(request.getBody().getJobOp().getData().getOwnerId());
		jobDesc.setJobId(request.getBody().getJobOp().getData().getJobId());
		jobDesc.setStatus(JobCode.JOBRECEIVED);
		NameValueSet.Builder nameValueSet = NameValueSet.newBuilder();
		nameValueSet.setName("Message");
		nameValueSet.setValue("The competition is held at :"
				+ request.getBody().getJobOp().getData().getOwnerId());
		nameValueSet.setNodeType(NodeType.VALUE);
		jobDesc.setOptions(nameValueSet);
		jobStatus.setJobState(JobCode.JOBRECEIVED);
		jobStatus.setStatus(PokeStatus.SUCCESS);
		jobStatus.addData(jobDesc);
		payLoad.setJobStatus(jobStatus);
		payLoad.build();
		rb.setBody(payLoad);

		Request reply = rb.build();

				
		return reply;

	}

}
	
