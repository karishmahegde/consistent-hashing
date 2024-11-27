package chNS;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Scanner;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.ObjectMapper;
 class Node {
	
	private String nodeId;
	
	private String ipAddress;

	private String portNumber;

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public String getPortNumber() {
		return portNumber;
	}

	public void setPortNumber(String portNumber) {
		this.portNumber = portNumber;
	}

	public Node(String nodeId, String ipAddress, String portNumber) {
		this.nodeId = nodeId;
		this.ipAddress = ipAddress;
		this.portNumber = portNumber;
	}

	public Node() {
		
	}

	@Override
	public String toString() {
		return "Node [nodeId=" + nodeId + ", ipAddress=" + ipAddress + ", portNumber=" + portNumber + "]";
	}

}

class NSConsistentHashingThread implements Runnable {
	
	/** Duties of this thread-
	 * 1. This thread will execute a while loop where each while iteration waits for socket incomming connection
	 * 2. 
	 */
	
	@Override
	public void run() {
		
		Thread.currentThread().setName("NS-Consistent-Hashing-Thread");
		while (true) {
			try (ServerSocket listener = new ServerSocket(Integer.parseInt(NSUserInteractionMain.self.getPortNumber()))) {
				/*
				 * Listen for a socket connection from either of the name servers
				 */
				Socket chSocket = listener.accept();
				
				OutputStream out = chSocket.getOutputStream();
	            InputStream in = chSocket.getInputStream();
	            PrintWriter chSocketOutPw = new PrintWriter(out,true);
	            Scanner chSocketInSc = new Scanner(in);
	            //Receive the command from other servers bootstrap server/name server 
	            String line = chSocketInSc.nextLine();
	            
				switch (line) {
				case AppConstants.ENTER:
					//Receive the ID of the new coming node (name server)
					String newNodeID = chSocketInSc.nextLine();
					//capture the IP address of the new coming node from the socket
					String newNodeIP = chSocketInSc.nextLine();
					//receive the port number of the new coming node 
					String newNodePort = chSocketInSc.nextLine();
					
					//Check if new node's responsibility belongs to this name server node
					if (NSUserInteractionMain.dataChunk.containsKey(Integer.parseInt(newNodeID))) { 
						//The new node is the self responsibility of this nameServer. Write self responsibility logic here#######@@@@@
						chSocketOutPw.println("No");
						
						//get the submap from the entire dataChunk that needs to be sent to the newly joined node 
						TreeMap<Integer, String> dataChunk = new TreeMap<Integer, String>(NSUserInteractionMain.dataChunk);
						SortedMap<Integer, String> transferDataChunk = dataChunk.subMap(dataChunk.firstKey(), Integer.parseInt(newNodeID)+1);
						
						//Update the nameServer's dataChunk as some data is distributed to the new node!
						for (Integer key: transferDataChunk.keySet()) {
							if (NSUserInteractionMain.dataChunk.containsKey(key)) {
								NSUserInteractionMain.dataChunk.remove(key);
							}
						}
						
						//Serialize the transferDataChunk from MAP to JSON String to send over socket
						String transferDataChunkJson = new ObjectMapper().writeValueAsString(transferDataChunk);
						
						Socket dataChunkSocket = null;
						PrintWriter dataChunkSocketOutPw = null;
						Scanner dataChunkSocketInSc = null;
						
						try {
							/**----- Open the socket to the newly joined node in order to send chunk of data which new node needs. -----*/
							dataChunkSocket = new Socket(newNodeIP, Integer.parseInt(newNodePort));
							dataChunkSocketOutPw = new PrintWriter(dataChunkSocket.getOutputStream(), true);
							dataChunkSocketInSc = new Scanner(dataChunkSocket.getInputStream());
							
							/*----- Send the dataChunk to the newly joined node -----*/
							//Sending the command
							dataChunkSocketOutPw.println(AppConstants.ADJ_KEYSPACE);
							//Sending the dataChunk
							dataChunkSocketOutPw.println(transferDataChunkJson);
							
							// Send the nameServer's prev node & its own self details to the new node.
							Node[] nodeArr = { NSUserInteractionMain.prev, NSUserInteractionMain.self };
							List<Node> nodes = Arrays.asList(nodeArr);
							for (Node node : nodes) {
								dataChunkSocketOutPw.println(node.getNodeId());
								dataChunkSocketOutPw.println(node.getIpAddress());
								dataChunkSocketOutPw.println(node.getPortNumber());
							}
							dataChunkSocketInSc.nextLine();
							
							//Now nameserver will update its prev pointer to point newly entered node
							NSUserInteractionMain.prev.setNodeId(newNodeID);
							NSUserInteractionMain.prev.setIpAddress(newNodeIP);
							NSUserInteractionMain.prev.setPortNumber(newNodePort);
							
							//Now inform the server(bootstrap/nameserver) which contacted this nameserver for node handling responsibility to update its pointers
							chSocketOutPw.println(newNodeID);
							chSocketOutPw.println(newNodeIP);
							chSocketOutPw.println(newNodePort);
							
							//Send the node ID for back tracing of visited nodes
							chSocketOutPw.println(NSUserInteractionMain.self.getNodeId());
							
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							dataChunkSocketOutPw.close();
							dataChunkSocketInSc.close();
							dataChunkSocket.close();
						}
							
					} else {
						//The new node is the responsibility of some successor node
						chSocketOutPw.println("Yes");
						//Open a new socket to this nameserver's successor and send the new coming node details to it.
						Socket chExplorerSocket = null;
						PrintWriter chExplorerSocketOutPw = null;
						Scanner chExplorerSocketInSc = null;
						try {
							//Open the socket to the successor of this nameserver to explore more
							chExplorerSocket = new Socket(NSUserInteractionMain.next.getIpAddress(), Integer.parseInt(NSUserInteractionMain.next.getPortNumber()));
							chExplorerSocketOutPw = new PrintWriter(chExplorerSocket.getOutputStream(), true);
							chExplorerSocketInSc = new Scanner(chExplorerSocket.getInputStream());
							
							/*---Send the new node details to the successor of this nameserver---*/
							chExplorerSocketOutPw.println(line);
							chExplorerSocketOutPw.println(newNodeID);
							chExplorerSocketOutPw.println(newNodeIP);
							chExplorerSocketOutPw.println(newNodePort);
							
							String exploreMore = chExplorerSocketInSc.nextLine();
							
							if (exploreMore.equals("Yes")) {
								// If's body is purposely kept blank because if the successor also sends the new coming node ahead in the chain of consistent hashing in that case,
								//this node's next pointer is not needed to be updated!!
							} else {
								//Set the name server next successor pointer to new node.							
								NSUserInteractionMain.next.setNodeId(chExplorerSocketInSc.nextLine());
								NSUserInteractionMain.next.setIpAddress(chExplorerSocketInSc.nextLine());
								NSUserInteractionMain.next.setPortNumber(chExplorerSocketInSc.nextLine());
							}
							
							String nodeEntryTrace = NSUserInteractionMain.self.getNodeId() +"," +chExplorerSocketInSc.nextLine();
							chSocketOutPw.println(nodeEntryTrace);
							
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							chExplorerSocketOutPw.close();
							chExplorerSocketInSc.close();
							chExplorerSocket.close();
						}	
					}	
					break;

				case AppConstants.EXIT:
					//Receive the mode of exit process
					String exitMode = chSocketInSc.nextLine();
					if (exitMode.equals("data+pointers")) {
						//Then we have to adjust the surrendered dataChunk from previous exiting node to this node's dataChunk
						String transferDataChunkJson = chSocketInSc.nextLine();
						
						// Set the received/surrendered dataChunk to this Node
						Map<?, ?> strMap = new ObjectMapper().readValue(transferDataChunkJson, LinkedHashMap.class);	
						for (Map.Entry<?, ?> strEntry : strMap.entrySet()) {
							NSUserInteractionMain.dataChunk.put(Integer.parseInt((String) strEntry.getKey()), (String) strEntry.getValue());
						}
						
						SortedMap<Integer, String> sortedDataChunk = new TreeMap<Integer, String> (NSUserInteractionMain.dataChunk);
						NSUserInteractionMain.dataChunk.clear();
						NSUserInteractionMain.dataChunk.putAll(sortedDataChunk);
						
						//Update the prev pointer of this node.
						NSUserInteractionMain.prev.setNodeId(chSocketInSc.nextLine());
						NSUserInteractionMain.prev.setIpAddress(chSocketInSc.nextLine());
						NSUserInteractionMain.prev.setPortNumber(chSocketInSc.nextLine());
						chSocketOutPw.println("done");
						
					} else {
						//Upate the next pointer of this node
						NSUserInteractionMain.next.setNodeId(chSocketInSc.nextLine());
						NSUserInteractionMain.next.setIpAddress(chSocketInSc.nextLine());
						NSUserInteractionMain.next.setPortNumber(chSocketInSc.nextLine());
						chSocketOutPw.println("done");
					}
					break;
					
				case AppConstants.INSERT:
					String insertKey = chSocketInSc.nextLine();
					String insertValue = chSocketInSc.nextLine();
					if (NSUserInteractionMain.dataChunk.containsKey(Integer.parseInt(insertKey))) {
						//The key for record insert is available with me(nameserver) only, hence update it and respond back to the prev node via already open socket
						NSUserInteractionMain.dataChunk.put(Integer.parseInt(insertKey), insertValue);
						
						//Respond back to the prev node
						chSocketOutPw.println(NSUserInteractionMain.dataChunk.get(Integer.parseInt(insertKey)));
						chSocketOutPw.println(NSUserInteractionMain.self.getNodeId());
					} else {
						//Record for lookup is not with this nameserver, hence delegate to further nodes for lookup
						//Open socket to next name server and pass the lookup responsibility
						Socket chExplorerSocket = null;
						PrintWriter chExplorerSocketOutPw = null;
						Scanner chExplorerSocketInSc = null;
						try {
							chExplorerSocket = new Socket(NSUserInteractionMain.next.getIpAddress(), Integer.parseInt(NSUserInteractionMain.next.getPortNumber()));
							chExplorerSocketOutPw = new PrintWriter(chExplorerSocket.getOutputStream(), true);
							chExplorerSocketInSc = new Scanner(chExplorerSocket.getInputStream());
							
							//Send the command to next name server in the chain
							chExplorerSocketOutPw.println(line);
							
							//Send the insert key and value to the next nameserver
							chExplorerSocketOutPw.println(insertKey);
							chExplorerSocketOutPw.println(insertValue);
							
							//Return the response from the next node after exploring is done
							chSocketOutPw.println(chExplorerSocketInSc.nextLine());
							chSocketOutPw.println(NSUserInteractionMain.self.getNodeId()+","+chExplorerSocketInSc.nextLine());
							
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							chExplorerSocketInSc.close();
							chExplorerSocketOutPw.close();
							chExplorerSocket.close();
						}
					}
					break;
					
				case AppConstants.LOOKUP:
					String lookupKey = chSocketInSc.nextLine();
					if (NSUserInteractionMain.dataChunk.containsKey(Integer.parseInt(lookupKey))) {
						//The record for lookup is available with me(nameserver) only, hence fetch it and return back to the prev node via already open socket
						chSocketOutPw.println(NSUserInteractionMain.dataChunk.get(Integer.parseInt(lookupKey)));
						chSocketOutPw.println(NSUserInteractionMain.self.getNodeId());
					} else {
						//Record for lookup is not with this nameserver, hence delegate to further nodes for lookup
						//Open socket to next name server and pass the lookup responsibility
						Socket chExplorerSocket = null;
						PrintWriter chExplorerSocketOutPw = null;
						Scanner chExplorerSocketInSc = null;
						try {
							chExplorerSocket = new Socket(NSUserInteractionMain.next.getIpAddress(), Integer.parseInt(NSUserInteractionMain.next.getPortNumber()));
							chExplorerSocketOutPw = new PrintWriter(chExplorerSocket.getOutputStream(), true);
							chExplorerSocketInSc = new Scanner(chExplorerSocket.getInputStream());
							
							//Send the command to next name server in the chain
							chExplorerSocketOutPw.println(line);
							
							//Send the lookup key to the next nameserver
							chExplorerSocketOutPw.println(lookupKey);
							
							//Return the response from the next node after exploring is done
							chSocketOutPw.println(chExplorerSocketInSc.nextLine());
							chSocketOutPw.println(NSUserInteractionMain.self.getNodeId()+","+chExplorerSocketInSc.nextLine());
							
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							chExplorerSocketInSc.close();
							chExplorerSocketOutPw.close();
							chExplorerSocket.close();
						}
					}
					break;
					
				case AppConstants.DELETE:
					String deleteKey = chSocketInSc.nextLine();
					
					if (NSUserInteractionMain.dataChunk.containsKey(Integer.parseInt(deleteKey))) {
						//The key for record insert is available with me(nameserver) only, hence update it and respond back to the prev node via already open socket
						NSUserInteractionMain.dataChunk.put(Integer.parseInt(deleteKey), null);
						
						//Respond back to the prev node
						chSocketOutPw.println(NSUserInteractionMain.dataChunk.get(Integer.parseInt(deleteKey)));
						chSocketOutPw.println(NSUserInteractionMain.self.getNodeId());
					} else {
						//Record for lookup is not with this nameserver, hence delegate to further nodes for lookup
						//Open socket to next name server and pass the lookup responsibility
						Socket chExplorerSocket = null;
						PrintWriter chExplorerSocketOutPw = null;
						Scanner chExplorerSocketInSc = null;
						try {
							chExplorerSocket = new Socket(NSUserInteractionMain.next.getIpAddress(), Integer.parseInt(NSUserInteractionMain.next.getPortNumber()));
							chExplorerSocketOutPw = new PrintWriter(chExplorerSocket.getOutputStream(), true);
							chExplorerSocketInSc = new Scanner(chExplorerSocket.getInputStream());
							
							//Send the command to next name server in the chain
							chExplorerSocketOutPw.println(line);
							
							//Send the insert key and value to the next nameserver
							chExplorerSocketOutPw.println(deleteKey);
							
							//Return the response from the next node after exploring is done
							chSocketOutPw.println(chExplorerSocketInSc.nextLine());
							chSocketOutPw.println(NSUserInteractionMain.self.getNodeId()+","+chExplorerSocketInSc.nextLine());
							
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							chExplorerSocketInSc.close();
							chExplorerSocketOutPw.close();
							chExplorerSocket.close();
						}
					}
					break;
					
				case AppConstants.ADJ_KEYSPACE:
					// Receive the dataChunk that belongs to this newly entered node
					String transferDataChunkJson = chSocketInSc.nextLine();
					
					// Set the dataChunk to this Node
					Map<?, ?> strMap = new ObjectMapper().readValue(transferDataChunkJson, LinkedHashMap.class);	
					for (Map.Entry<?, ?> strEntry : strMap.entrySet()) {
						NSUserInteractionMain.dataChunk.put(Integer.parseInt((String) strEntry.getKey()), (String) strEntry.getValue());
					}

					// Start pointing to the next and prev nodes as informed
					NSUserInteractionMain.prev.setNodeId(chSocketInSc.nextLine());
					NSUserInteractionMain.prev.setIpAddress(chSocketInSc.nextLine());
					NSUserInteractionMain.prev.setPortNumber(chSocketInSc.nextLine());
					
					NSUserInteractionMain.next.setNodeId(chSocketInSc.nextLine());
					NSUserInteractionMain.next.setIpAddress(chSocketInSc.nextLine());
					NSUserInteractionMain.next.setPortNumber(chSocketInSc.nextLine());
					chSocketOutPw.println("ok");
					break;
					
				case AppConstants.PTR_UPDATE_BN:
					//Set the details of the new node as the successor to this node.
					//Receive the ID of the new coming node (name server)
					String newNxtNodeID = chSocketInSc.nextLine();
					//capture the IP address of the new coming node from the socket
					String newNxtNodeIP = chSocketInSc.nextLine();
					//receive the port number of the new coming node 
					String newNxtNodePort = chSocketInSc.nextLine();
					
					//Update the next(successor) pointer of this name server to point the newly added node
					NSUserInteractionMain.next.setNodeId(newNxtNodeID);
					NSUserInteractionMain.next.setIpAddress(newNxtNodeIP);
					NSUserInteractionMain.next.setPortNumber(newNxtNodePort);
					
					break;

				default:
					break;
				}

				chSocketInSc.close();
				chSocketOutPw.close();
				chSocket.close();  
	            
			} catch (IOException e) {		
				e.printStackTrace();
			}
		
		}
		
	}

}
 class AppUtil {
	
	// To get the response from the server
	public static String getPrintWriterResponse(String line, Scanner socketInSc) {
        while (true) {
            line = socketInSc.nextLine();
            if (line.equals("") || line.equals("goodbye")) {
                return line;
            }
            System.out.println("Coordinator response: " + line);
        }
    }
	
	public static String getPrintWriterResponseRegistration(String line, Scanner socketInSc) {
		String coordinatorRes = null;
		while (true) {
            line = socketInSc.nextLine();
            if (line.equals("") || line.equals("goodbye")) {
                return coordinatorRes;
            }
            coordinatorRes = line;
            System.out.println("Coordinator response: " + line);
        }
    }
	
	public static boolean checkPattern(String inputPattern, String data) {
		Pattern pattern = Pattern.compile(inputPattern);		
		Matcher matcher = pattern.matcher(data);
		return matcher.matches();
	}
}

 class AppConstants {
	
	/*-- The constants for switch cases --*/
	public static final String INSERT = "insert";
	public static final String LOOKUP = "lookup";
	public static final String DELETE = "delete";
	public static final String ENTER = "enter";
	public static final String EXIT = "exit";
	public static final String ADJ_KEYSPACE = "adjustKeySpace";
	public static final String PTR_UPDATE_BN = "ptrUpdateBn";
	
	/*-- The console cursor for user inputs --*/
	public static final String NAME_SERVER = "nameserver ";
	
	private AppConstants () {
		
	}

}

public class NSUserInteractionMain {
	
	public static Node prev;
	
	public static Node self;
	
	public static Node next;
	
	public static Node bootstrapNode;
	
	public static LinkedHashMap<Integer, String> dataChunk;
	
	public static Properties appProperties;

	static {
		prev = new Node();
		self = new Node();
		next = new Node();
		bootstrapNode = new Node();
		dataChunk = new LinkedHashMap<Integer, String>();
		appProperties = new Properties();
	}
	
	public static void main(String[] args) throws IOException {
	
		//On startup read the config
		if (args.length != 1) {
			System.err.println("Pass the config file name for nameserver execution as the command line argument only");
			return;
		}
		
		/** --------Load the properties file into the application-------- */
		//File propertyFile = new File("ns-config.txt");
		File propertyFile = new File(args[0]);
		if (!propertyFile.exists()) {
			System.err.println("The property file with the given name does not exists");
			return;
		}
		FileInputStream fis = new FileInputStream(propertyFile);
		try {
			appProperties.load(fis);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fis.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		//Set the self node details to the above Node objects
		self.setNodeId(appProperties.getProperty("nameserver.id"));
		self.setIpAddress(InetAddress.getLocalHost().getHostAddress());
		self.setPortNumber(appProperties.getProperty("nameserver.port"));
		
		//Set the connection details of the bnserver/bootstrap server
		bootstrapNode.setIpAddress(appProperties.getProperty("bnserver.ip"));
		bootstrapNode.setPortNumber(appProperties.getProperty("bnserver.port"));
		
		//Start the ConsistentHashingThread
		NSConsistentHashingThread chThread = new NSConsistentHashingThread();
		Thread t = new Thread(chThread);
		t.start();    
        
		Scanner userInSc = null;
		try {
			//Define line & scanner to take user commands
			String line = "";
			userInSc = new Scanner(System.in);
			while (true) { // Wait for the user command
				System.out.print(AppConstants.NAME_SERVER+self.getNodeId()+">");
				line = userInSc.nextLine(); // what we entered
				System.out.println("You entered " + line);

				switch (line) {
				case AppConstants.ENTER:
					Socket bnSocket = null;
					PrintWriter bnSocketOutPw = null;
					Scanner bnSocketInSc = null;
					try {
						bnSocket = new Socket(bootstrapNode.getIpAddress(),
								Integer.parseInt(bootstrapNode.getPortNumber()));
						bnSocketOutPw = new PrintWriter(bnSocket.getOutputStream(), true);
						bnSocketInSc = new Scanner(bnSocket.getInputStream());

						//Send the command to bnserver
						bnSocketOutPw.println(line);

						//Send this new coming node's details to the bnserver
						bnSocketOutPw.println(self.getNodeId());
						bnSocketOutPw.println(self.getIpAddress());
						bnSocketOutPw.println(self.getPortNumber());

						//Get the tracing of the ID's of the server's that were traversed while installing this node.
						String temp=bnSocketInSc.nextLine();
						System.out.println(temp);
						//String[] trace = bnSocketInSc.nextLine().split(",");
						String[] trace = temp.split(",");

						//Print the bootstrap server response
						System.out.println("Range: " + NSUserInteractionMain.dataChunk.keySet().toArray()[0] + "-"
								+ NSUserInteractionMain.dataChunk.keySet().toArray()[NSUserInteractionMain.dataChunk.size()
										- 1]);
						System.out.println("Predecessor: " + prev.getNodeId());
						System.out.println("Successor: " + next.getNodeId());
						System.out.println("Visited Severs: " + Arrays.asList(trace));
						System.out.println("Succesful Entry");

					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						bnSocketInSc.close();
						bnSocketOutPw.close();
						bnSocket.close();
					}
					break;

				case AppConstants.EXIT:
					//Begin the gracefull exit procedure by connecting successor and surrender this node's dataChunk to successor
					Socket exitSocket = null;
					PrintWriter exitSocketOutPw = null;
					Scanner exitSocketInSc = null;
					String exitResponse1 = null;
					String exitResponse2 = null;
					//Connect to successor and send dataChunk+pointers
					try {
						exitSocket = new Socket(next.getIpAddress(), Integer.parseInt(next.getPortNumber()));
						exitSocketOutPw = new PrintWriter(exitSocket.getOutputStream(), true);
						exitSocketInSc = new Scanner(exitSocket.getInputStream());

						// Send the command to successor node
						exitSocketOutPw.println(line);
						//Send the behaviour of successor's exit process
						exitSocketOutPw.println("data+pointers");

						//Send (surrender) the dataChunk to the successor node
						String transferDataChunkJson = new ObjectMapper().writeValueAsString(dataChunk);
						exitSocketOutPw.println(transferDataChunkJson);

						//Send the pointers of this node's prev node to the successor
						exitSocketOutPw.println(prev.getNodeId());
						exitSocketOutPw.println(prev.getIpAddress());
						exitSocketOutPw.println(prev.getPortNumber());

						exitResponse1 = exitSocketInSc.nextLine();
						System.out.println("Successor Node "+next.getNodeId());
						System.out.println("Range handed over "+(prev.getNodeId()+1)+"-"+self.getNodeId());
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						exitSocketInSc.close();
						exitSocketOutPw.close();
						exitSocket.close();
					}

					//Connect to predecessor and send pointers
					try {
						exitSocket = new Socket(prev.getIpAddress(), Integer.parseInt(prev.getPortNumber()));
						exitSocketOutPw = new PrintWriter(exitSocket.getOutputStream(), true);
						exitSocketInSc = new Scanner(exitSocket.getInputStream());

						// Send the command to successor node
						exitSocketOutPw.println(line);
						//Send the behaviour of predecesspor's exit process
						exitSocketOutPw.println("pointers");

						//Send the pointers of this node's next node to the predecessor
						exitSocketOutPw.println(next.getNodeId());
						exitSocketOutPw.println(next.getIpAddress());
						exitSocketOutPw.println(next.getPortNumber());

						exitResponse2 = exitSocketInSc.nextLine();
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						exitSocketInSc.close();
						exitSocketOutPw.close();
						exitSocket.close();
					}

					//Now refresh the pointers and dataChunk of this exiting node
					prev.setNodeId(null);
					prev.setIpAddress(null);
					prev.setPortNumber(null);

					next.setNodeId(null);
					next.setIpAddress(null);
					next.setPortNumber(null);

					dataChunk.clear();

					if (exitResponse1.equals("done") && exitResponse2.equals("done")) {
						System.out.println("Exit successful");
					}
					break;

				default:
					System.out.println("Invalid Command.....");
					break;

				}
			} 
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			userInSc.close();
		}

	}
	
}