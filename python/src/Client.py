# Client sends the request to the server
# The functionalities like signup, listcourses, getdescription and competition are included
# Abinaya Sampath

import comm_pb2
import sys
import google
import socket
import struct
import time

HOST= 'localhost'
PORT=5570
req=comm_pb2.Request()
header = req.header
body = req.body
global msg
res=comm_pb2.Response()


def buildHeader(routingId):	
    header.originator = "client"
    #time in milliseconds
    millis = int(round(time.time() * 1000))
    header.time = millis
    header.routing_id = routingId;
    return header

def sendRequest(req):
	for res in socket.getaddrinfo(HOST, PORT, socket.AF_UNSPEC, socket.SOCK_STREAM):
		af, socktype, proto, canonname, address = res
		try:
			global sock
        	    	sock = socket.socket(af, socktype, proto)
		except OSError,msg:
			sock = None
        	    	continue
		try:
			sock.connect(address)
		except OSError,msg:
			sock.close()
			sock = None
			continue
			break
	if sock is None:
		print('could not open socket')
		sys.exit(1)
    
	str = req.SerializeToString()
	packed_len = struct.pack('>L', len(str))
	sock.sendall(packed_len + str)

def readList():
	msg=''
	len_buf = readMessage(4)
	msg_len = struct.unpack('>L', len_buf)[0]
	msg_buf = readMessage(msg_len)
	res.ParseFromString(msg_buf)
	for i in range(0,len(res.body.job_status.data[0].options.node)):			
		print("CourseName: " +res.body.job_status.data[0].options.node[i].value)
		i+=1
	return
	
def readDescription():
	msg=''
	len_buf = readMessage(4)
	msg_len = struct.unpack('>L', len_buf)[0]
	msg_buf = readMessage(msg_len)
	res.ParseFromString(msg_buf)						
	print("CourseDescription: " +res.body.job_status.data[0].options.value)
	return
	
def getSignUpResponse():
	msg=''
	len_buf = readMessage(4)
	msg_len = struct.unpack('>L', len_buf)[0]
	msg_buf = readMessage(msg_len)
	res.ParseFromString(msg_buf)						
	print res.body.job_status.data[0].options.value
	return
	
def readCompetitionResult():
	msg=''
	len_buf = readMessage(4)
	msg_len = struct.unpack('>L', len_buf)[0]
	msg_buf = readMessage(msg_len)
	res.ParseFromString(msg_buf)						
	print("Competition: " +res.body.job_status.data[0].options.value)
	return
	
def readMessage(n):
	buffer = ''
	while n>0 :
		data = sock.recv(n)
		if data == '':
			print "No data found"
		buffer+=data
		n-=len(data)
		return buffer	

def addUser():
	print "Add the user"
	header=buildHeader(comm_pb2.Header.JOBS)
	body.job_op.data.name_space="signup"
	body.job_op.action=1
	body.job_op.data.owner_id=22
	body.job_op.data.job_id="Abisignup"
	body.job_op.data.status=4
	body.job_op.data.options.node_type=1
	nodeEmail=body.job_op.data.options.node.add()

	nodeEmail.name="email"
	nodeEmail.value=raw_input("Enter the email id : ")
	nodeEmail.node_type=2
	
	nodePassword=body.job_op.data.options.node.add()
	nodePassword.name="password"
	nodePassword.value=raw_input("Enter the Password : ")
	nodePassword.node_type=2
	
	nodeFName=body.job_op.data.options.node.add()
	nodeFName.name="fname"
	nodeFName.value=raw_input("Enter the First Name : ")
	nodeFName.node_type=2
	
	nodeLName=body.job_op.data.options.node.add()
	nodeLName.name="lname"
	nodeLName.value=raw_input("Enter the Last Name : ")
	nodeLName.node_type=2
	
	sendToChannel(req)
	getSignUpResponse()
	return

def listCourses():
	header=buildHeader(comm_pb2.Header.JOBS)
	body.job_op.data.name_space="listcourses"
 	body.job_op.data.owner_id=21
	body.job_op.data.job_id="list"
	body.job_op.data.status=4
	body.job_op.data.options.node_type=2
	body.job_op.action=1
	sendRequest(req)
	readList()
	return

def getDescription():
	print "Description hurray"
	header=buildHeader(comm_pb2.Header.JOBS)
	body.job_op.data.name_space="getdescription"
 	body.job_op.data.owner_id=21
	body.job_op.data.job_id="description"
	body.job_op.data.status=4
	body.job_op.data.options.node_type=2
	body.job_op.data.options.name="coursename"
	body.job_op.data.options.value=raw_input("Enter the course name:")
	body.job_op.action=4
	sendRequest(req)
	readDescription()
	return
	

def competition():
	header=buildHeader(comm_pb2.Header.JOBS)
	body.job_op.data.name_space="competition"
 	body.job_op.data.owner_id=21
	body.job_op.data.job_id="Abitest"
	body.job_op.data.status=4
	body.job_op.action=4
	sendRequest(req)
	readDescription()
	return

print "Select an option"
print "1. New User??? Sign Up"
print "2. List the Courses"
print "3. Get the description of a selected course"
print "4. Competition"
print "5.Exit"

while True:

    OPTION = int(raw_input("CHOOSE AN OPTION: ")) 

    if OPTION == 1: 
        print "SIGN UP"
        addUser()

    elif OPTION == 2:
        print "THE FOLLOWING COURSES ARE OFFERED"
        listCourses()
        continue

    elif OPTION == 3:
        print "GET THE DESCRIPTION "
        getDescription()

    elif OPTION == 4:
        print "COMPETITION"
        print competition()
        
    elif OPTION == 5:
    	exit()
    	
        
        
