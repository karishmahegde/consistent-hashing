package chBS;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
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
//import com.fasterxml.jackson.databind.ObjectMapper;
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
 class BSConsistentHashingThread implements Runnable {

	@Override
	public void run() {
		
		/** Duties of this thread-
		 * 1. This thread will execute a while loop where each while iteration waits for socket incomming connection
		 * 2. 
		 */
		
		Thread.currentThread().setName("BS-Consistent-Hashing-Thread");
		while (true) {
			try (ServerSocket listener = new ServerSocket(Integer.parseInt(BSUserInteractionMain.self.getPortNumber()))) {
				/*
				 * Listen for a socket connection from either of the name servers
				 */
				Socket chSocket = listener.accept();
				
				OutputStream out = chSocket.getOutputStream();
	            InputStream in = chSocket.getInputStream();
	            PrintWriter chSocketOutPw = new PrintWriter(out,true);
	            Scanner chSocketInSc = new Scanner(in);
	            String line = chSocketInSc.nextLine();
	            
				switch (line) {
				case AppConstants.ENTER:
					//Receive the ID of the new coming node (name server)
					String newNodeID = chSocketInSc.nextLine();
					//capture the IP address of the new coming node from the socket
					String newNodeIP = chSocketInSc.nextLine();
					//receive the port number of the new coming node 
					String newNodePort = chSocketInSc.nextLine();
					
					if(BSUserInteractionMain.dataChunk.containsKey(Integer.parseInt(newNodeID))) {
						//The new node is the self responsibility of bnserver(bootstrap node). Write self responsibility logic here#######@@@@@
						//get the submap from the entire dataChunk that needs to be sent to the newly joined node 
						TreeMap<Integer, String> dataChunk = new TreeMap<Integer, String>(BSUserInteractionMain.dataChunk);
						SortedMap<Integer, String> transferDataChunk = dataChunk.subMap((Integer) dataChunk.keySet().toArray()[1], Integer.parseInt(newNodeID)+1);
						
						//Update the bnserver's (bootstrap node's) dataChunk as some data is distributed to the new node!
						for (Integer key: transferDataChunk.keySet()) {
							if (BSUserInteractionMain.dataChunk.containsKey(key)) {
								BSUserInteractionMain.dataChunk.remove(key);
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
							
							/*----- Update the pointers of the new node accordingly -----*/
							if (BSUserInteractionMain.prev.getNodeId() != null && !BSUserInteractionMain.prev.getNodeId().equals("0")) { // If the bnserver(bootstrap node) has a predecessor(previous node) already

								// Send the bnserver's prev node & its own self details to the new node.
								Node[] nodeArr = { BSUserInteractionMain.prev, BSUserInteractionMain.self };
								List<Node> nodes = Arrays.asList(nodeArr);
								for (Node node : nodes) {
									dataChunkSocketOutPw.println(node.getNodeId());
									dataChunkSocketOutPw.println(node.getIpAddress());
									dataChunkSocketOutPw.println(node.getPortNumber());
								}

								// Open the socket to the prev node and update its sucessor pointer to point the newly entered node.
								Socket prevSocket = null;
								PrintWriter prevSocketOutPw = null;
								Scanner prevSocketInSc = null;
								try {
									prevSocket = new Socket(BSUserInteractionMain.prev.getIpAddress(), Integer.parseInt(BSUserInteractionMain.prev.getPortNumber()));
									prevSocketOutPw = new PrintWriter(prevSocket.getOutputStream(), true);
									prevSocketInSc = new Scanner(prevSocket.getInputStream());
									
									//Send the details of newly entered node to the prev node of bnserver (bootstrap server) so that prev node can point to its new successor
									prevSocketOutPw.println(AppConstants.PTR_UPDATE_BN);
									prevSocketOutPw.println(newNodeID);
									prevSocketOutPw.println(newNodeIP);
									prevSocketOutPw.println(newNodePort);
									
									// After receiving the done from the prev node, the bnserver will update its prev pointer to point new node
									BSUserInteractionMain.prev.setNodeId(newNodeID);
									BSUserInteractionMain.prev.setIpAddress(newNodeIP);
									BSUserInteractionMain.prev.setPortNumber(newNodePort);
									
								} catch (Exception e) {
									e.printStackTrace();
								} finally {
									prevSocketOutPw.close();
									prevSocketInSc.close();
									prevSocket.close();
								}	
								
							} else { // If the bnserver(bootstrap node) does not have any predecessor(previous node)
								//Send the bnserver's details only, but twice to the new node.
								Node[] nodeArr = {BSUserInteractionMain.self, BSUserInteractionMain.self};
								List<Node> nodes = Arrays.asList(nodeArr);
								for (Node node: nodes) {
									dataChunkSocketOutPw.println(node.getNodeId());
									dataChunkSocketOutPw.println(node.getIpAddress());
									dataChunkSocketOutPw.println(node.getPortNumber());
								}
								//update bnserver's own prev and next pointer to point the newly entered node
								BSUserInteractionMain.prev.setNodeId(newNodeID);
								BSUserInteractionMain.prev.setIpAddress(newNodeIP);
								BSUserInteractionMain.prev.setPortNumber(newNodePort);
								
								BSUserInteractionMain.next.setNodeId(newNodeID);
								BSUserInteractionMain.next.setIpAddress(newNodeIP);
								BSUserInteractionMain.next.setPortNumber(newNodePort);
							}
							dataChunkSocketInSc.nextLine();
							chSocketOutPw.println(BSUserInteractionMain.self.getNodeId());
							
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							dataChunkSocketOutPw.close();
							dataChunkSocketInSc.close();
							dataChunkSocket.close();
						}	
						
					} else {
						//The new node is the responsibility of some successor node
						//Open a new socket to bnserver's successor and send the new coming node details to it.
						Socket chExplorerSocket = null;
						PrintWriter chExplorerSocketOutPw = null;
						Scanner chExplorerSocketInSc = null;
						try {
							//Open the socket to the successor of this bnserver to explore more
							chExplorerSocket = new Socket(BSUserInteractionMain.next.getIpAddress(), Integer.parseInt(BSUserInteractionMain.next.getPortNumber()));
							chExplorerSocketOutPw = new PrintWriter(chExplorerSocket.getOutputStream(), true);
							chExplorerSocketInSc = new Scanner(chExplorerSocket.getInputStream());
							
							/*---Send the new node details to the successor of bnserver(bootstrap node)---*/
							chExplorerSocketOutPw.println(line);
							chExplorerSocketOutPw.println(newNodeID);
							chExplorerSocketOutPw.println(newNodeIP);
							chExplorerSocketOutPw.println(newNodePort);
							
							String exploreMore = chExplorerSocketInSc.nextLine();
							
							if (exploreMore.equals("Yes")) {
								// If's body is purposely kept blank because if the successor also sends the new coming node ahead in the chain of consistent hashing in that case,
								//this node's next pointer is not needed to be updated!!
							} else {
								//Set the bootstrap server next successor pointer to new node.							
								BSUserInteractionMain.next.setNodeId(chExplorerSocketInSc.nextLine());
								BSUserInteractionMain.next.setIpAddress(chExplorerSocketInSc.nextLine());
								BSUserInteractionMain.next.setPortNumber(chExplorerSocketInSc.nextLine());
							}
							
							String nodeEntryTrace = BSUserInteractionMain.self.getNodeId() +"," +chExplorerSocketInSc.nextLine();
							//System.out.println(nodeEntryTrace);
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
							BSUserInteractionMain.dataChunk.put(Integer.parseInt((String) strEntry.getKey()), (String) strEntry.getValue());
						}
						
						SortedMap<Integer, String> sortedDataChunk = new TreeMap<Integer, String> (BSUserInteractionMain.dataChunk);
						
						Map.Entry<Integer, String> firstEntry = ((TreeMap<Integer, String>) sortedDataChunk).firstEntry();
						sortedDataChunk.remove(firstEntry.getKey());
						
						BSUserInteractionMain.dataChunk.clear();
						BSUserInteractionMain.dataChunk.putAll(sortedDataChunk);
						BSUserInteractionMain.dataChunk.put(firstEntry.getKey(), firstEntry.getValue());
						
						//Update the prev pointer of this node.
						BSUserInteractionMain.prev.setNodeId(chSocketInSc.nextLine());
						BSUserInteractionMain.prev.setIpAddress(chSocketInSc.nextLine());
						BSUserInteractionMain.prev.setPortNumber(chSocketInSc.nextLine());
						chSocketOutPw.println("done");
					} else {
						//Upate the next pointer of this node
						BSUserInteractionMain.next.setNodeId(chSocketInSc.nextLine());
						BSUserInteractionMain.next.setIpAddress(chSocketInSc.nextLine());
						BSUserInteractionMain.next.setPortNumber(chSocketInSc.nextLine());	
						chSocketOutPw.println("done");
					}
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
	public static final String BOOTSTRAP_SERVER = "bnserver> ";
	
	private AppConstants () {
		
	}

}
public class BSUserInteractionMain {
	
	public static Node prev;
	
	public static Node self;
	
	public static Node next;

	public static LinkedHashMap<Integer, String> dataChunk;
	
	public static Properties appProperties;
	
	public static Properties appData;
	
	static {
		prev = new Node();
		self = new Node();
		next = new Node();
		dataChunk = new LinkedHashMap<Integer, String>();
		appProperties = new Properties();
		appData = new Properties();
	}
	
	public static void main(String[] args) throws Exception {
	
		//On startup read both the config files
		if (args.length != 2) {
			System.err.println("Pass the config file name and data file name for bnserver execution as the command line argument only");
			return;
		}
		
		/** --------Load the properties file and data file into the application-------- */
		//File propertyFile = new File("bs-config.txt");
		//File dataFile = new File("data.txt");
		File propertyFile = new File(args[0]);
		File dataFile = new File(args[1]);
		if (!propertyFile.exists()) {
			System.err.println("The property file with the given name does not exists");
			return;
		}
		if (!dataFile.exists()) {
			System.err.println("The data file with the given name does not exists");
			return;
		}
		
		FileInputStream fis = new FileInputStream(propertyFile);
		FileInputStream fis2 = new FileInputStream(dataFile);
		try {
			appProperties.load(fis);
			appData.load(fis2);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fis.close();
				fis2.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		// Set the self node details to the above Node objects
		self.setNodeId(appProperties.getProperty("bnserver.id"));
		self.setIpAddress(InetAddress.getLocalHost().getHostAddress());
		self.setPortNumber(appProperties.getProperty("bnserver.port"));
		
		//Initialize the keyspace(dataChunk) of bnserver
		for (int i = 1; i < 1024; i++) {
			dataChunk.put(i, null);
		}
		dataChunk.put(0, null);
		
		//Update the null initialized dataChunk with the data read from the datafile.txt
		Map<Object, Object> dataMap = new LinkedHashMap<Object, Object>(appData);
		for (Map.Entry<Object, Object> dataEntry : dataMap.entrySet()) {
			dataChunk.put(Integer.parseInt((String) dataEntry.getKey()), (String) dataEntry.getValue());
		}
		
		// Start the ConsistentHashingThread
		BSConsistentHashingThread chThread = new BSConsistentHashingThread();
		Thread t = new Thread(chThread);
		t.start();
		
		Scanner userInSc = null;
		try {
			//Define line & scanner to take user commands
			String line = "";
			userInSc = new Scanner(System.in);
			
			while (true) { // Wait for the user command
				System.out.print(AppConstants.BOOTSTRAP_SERVER);
				
				line = userInSc.nextLine(); // what we entered
				System.out.println("You entered " + line);
				String[] commandArr = line.split(" ", 3);

				switch (commandArr[0]) {
				
				case AppConstants.LOOKUP:
					if (AppUtil.checkPattern("^lookup [0-9]{1,4}[:.,-]?$", line)) {
						if ((Integer.parseInt(commandArr[1]) >= 0) && (Integer.parseInt(commandArr[1]) <= 1023)) {
							String lookupKey = commandArr[1];
							if (dataChunk.containsKey(Integer.parseInt(lookupKey))) {
								//The record for lookup is available with me(bnserver) only, hence fetch it and return back to the user
								System.out.println(lookupKey + " : " + dataChunk.get(Integer.parseInt(lookupKey)));
								System.out.println("Visited Servers " + Arrays.asList(self.getNodeId().split(",")));
								//System.out.println(dataChunk.get(Integer.parseInt(lookupKey)));
								if(dataChunk.get(Integer.parseInt(lookupKey))!=null) {
									System.out.println("Result was found in server 0");
								}
								else {
									System.out.println("Key not found");	
									}
								
								
							} else {
								//Record for lookup is not with bnserver, hence delegate to further nodes for lookup
								//Open socket to next name server and pass the lookup responsibility
								Socket chExplorerSocket = null;
								PrintWriter chExplorerSocketOutPw = null;
								Scanner chExplorerSocketInSc = null;
								try {
									chExplorerSocket = new Socket(next.getIpAddress(),
											Integer.parseInt(next.getPortNumber()));
									chExplorerSocketOutPw = new PrintWriter(chExplorerSocket.getOutputStream(), true);
									chExplorerSocketInSc = new Scanner(chExplorerSocket.getInputStream());

									//Send the command to next name server in the chain
									chExplorerSocketOutPw.println(commandArr[0]);

									//Send the lookup key to the next nameserver
									chExplorerSocketOutPw.println(lookupKey);

									//Print the response from the next node after exploring is done
									String result=chExplorerSocketInSc.nextLine();
									
//									System.out.println(result);
//									System.out.println(result.length());
									if(result.equals("null")) {
										result="Key not found";
									}
									//System.out.println(lookupKey + " : " + chExplorerSocketInSc.nextLine());
									System.out.println(lookupKey + " : " + result);
									List<String> visitedServers=Arrays.asList(
											(self.getNodeId() + "," + chExplorerSocketInSc.nextLine()).split(","));
									String serversVisited="";
									System.out.print("Visited Servers:");
									for(int i=0;i<visitedServers.size();i++ ) {
										if(i==visitedServers.size()-1) {
											System.out.println(visitedServers.get(i));
											if(result.equals("Key not found")==false) {
												System.out.println("Result was found in server "+visitedServers.get(i));
											}
											
											
										}else {
											System.out.print(visitedServers.get(i)+"->");
										}
										
									}
//									System.out.println("Node Trace: " + Arrays.asList(
//											(self.getNodeId() + "," + chExplorerSocketInSc.nextLine()).split(",")));

								} catch (Exception e) {
									e.printStackTrace();
								} finally {
									chExplorerSocketInSc.close();
									chExplorerSocketOutPw.close();
									chExplorerSocket.close();
								}
							}
						} else {
							System.out.println("Key not found");
						} 
					} else {
						System.out.println("Invalid Command....");
					}
					break;
					
				case AppConstants.INSERT:
					if (AppUtil.checkPattern("^insert [0-9]{1,4}[:.,-]? [a-zA-Z0-9_]*$", line)) {
						if ((Integer.parseInt(commandArr[1]) >= 0) && (Integer.parseInt(commandArr[1]) <= 1023)) {
							String insertKey = commandArr[1];
							String insertValue = commandArr[2];
							if (dataChunk.containsKey(Integer.parseInt(insertKey))) {
								//The key for insert is available with me(bnserver) only, hence update it and return back to the user
								dataChunk.put(Integer.parseInt(insertKey), insertValue);
								System.out.println(insertKey + " : " + dataChunk.get(Integer.parseInt(insertKey)));
								System.out.println("Visited Servers: " + Arrays.asList(self.getNodeId().split(",")));
								System.out.println("Key value pair successfully inserted into server 0");
							} else {
								//Record for lookup is not with bnserver, hence delegate to further nodes for lookup
								//Open socket to next name server and pass the lookup responsibility
								Socket chExplorerSocket = null;
								PrintWriter chExplorerSocketOutPw = null;
								Scanner chExplorerSocketInSc = null;
								try {
									chExplorerSocket = new Socket(next.getIpAddress(),
											Integer.parseInt(next.getPortNumber()));
									chExplorerSocketOutPw = new PrintWriter(chExplorerSocket.getOutputStream(), true);
									chExplorerSocketInSc = new Scanner(chExplorerSocket.getInputStream());

									//Send the command to next name server in the chain
									chExplorerSocketOutPw.println(commandArr[0]);

									//Send the insert key to the next nameserver
									chExplorerSocketOutPw.println(insertKey);
									chExplorerSocketOutPw.println(insertValue);

									//Print the response from the next node after exploring is done
									System.out.println(insertKey + " : " + chExplorerSocketInSc.nextLine());
									List<String> visitedServers=Arrays.asList(
											(self.getNodeId() + "," + chExplorerSocketInSc.nextLine()).split(","));
									String serversVisited="";
									System.out.print("Visited Servers:");
									for(int i=0;i<visitedServers.size();i++ ) {
										if(i==visitedServers.size()-1) {
											System.out.println(visitedServers.get(i));
											
											System.out.println("Key Value pair was successfully inserted into server "+visitedServers.get(i));
											
											
											
										}else {
											System.out.print(visitedServers.get(i)+"->");
										}
										
									}
//									System.out.println("Visited Servers: " + Arrays.asList(
//											(self.getNodeId() + "," + chExplorerSocketInSc.nextLine()).split(",")));

								} catch (Exception e) {
									e.printStackTrace();
								} finally {
									chExplorerSocketInSc.close();
									chExplorerSocketOutPw.close();
									chExplorerSocket.close();
								}
							}
						} else {
							System.out.println("Key not found");
						} 
					} else {
						System.out.println("Invalid Command....");
					}
					break;
				
				case AppConstants.DELETE:
					if (AppUtil.checkPattern("^delete [0-9]{1,4}[:.,-]?$", line)) {
						if ((Integer.parseInt(commandArr[1]) >= 0) && (Integer.parseInt(commandArr[1]) <= 1023)) {
							
							//check if the given key exists in the system
							
							String lookupKey = commandArr[1];
							if (dataChunk.containsKey(Integer.parseInt(lookupKey))) {
								//The record for lookup is available with me(bnserver) only, hence fetch it and return back to the user
//								System.out.println(lookupKey + " : " + dataChunk.get(Integer.parseInt(lookupKey)));
//								System.out.println("Visited Servers " + Arrays.asList(self.getNodeId().split(",")));
//								System.out.println("Result was found in server 0");
								if(dataChunk.get(Integer.parseInt(lookupKey))!=null) {
									//System.out.println("Result was found in server 0");
								}
								else {
									System.out.println("Key not found");	
									break;
									}
								
							} else {
								//Record for lookup is not with bnserver, hence delegate to further nodes for lookup
								//Open socket to next name server and pass the lookup responsibility
								Socket chExplorerSocket = null;
								PrintWriter chExplorerSocketOutPw = null;
								Scanner chExplorerSocketInSc = null;
								try {
									chExplorerSocket = new Socket(next.getIpAddress(),
											Integer.parseInt(next.getPortNumber()));
									chExplorerSocketOutPw = new PrintWriter(chExplorerSocket.getOutputStream(), true);
									chExplorerSocketInSc = new Scanner(chExplorerSocket.getInputStream());

									//Send the command to next name server in the chain
									chExplorerSocketOutPw.println("lookup");

									//Send the lookup key to the next nameserver
									chExplorerSocketOutPw.println(lookupKey);

									//Print the response from the next node after exploring is done
									String result=chExplorerSocketInSc.nextLine();
									
									System.out.println(result);
									System.out.println(result.length());
									if(result.equals("null")) {
										result="Key not found";
									}
									//System.out.println(lookupKey + " : " + chExplorerSocketInSc.nextLine());
//									System.out.println(lookupKey + " : " + result);
//									List<String> visitedServers=Arrays.asList(
//											(self.getNodeId() + "," + chExplorerSocketInSc.nextLine()).split(","));
//									String serversVisited="";
//									System.out.print("Visited Servers:");
//									for(int i=0;i<visitedServers.size();i++ ) {
//										if(i==visitedServers.size()-1) {
//											System.out.println(visitedServers.get(i));
////											if(result.equals("Key not found")==false) {
////												System.out.println("Result was found in server "+visitedServers.get(i));
////											}
//											
//											
//										}else {
//											System.out.print(visitedServers.get(i)+"->");
//										}
//										
									//}
									if(result.equals("Key not found")) {
										System.out.println("Key not found");
										break;
									}
//									System.out.println("Node Trace: " + Arrays.asList(
//											(self.getNodeId() + "," + chExplorerSocketInSc.nextLine()).split(",")));

								} catch (Exception e) {
									e.printStackTrace();
								} finally {
									chExplorerSocketInSc.close();
									chExplorerSocketOutPw.close();
									chExplorerSocket.close();
								}
							}
							
							//finished checking for the given key in the system
							String deleteKey = commandArr[1];
							if (dataChunk.containsKey(Integer.parseInt(deleteKey))) {
								//The key for insert is available with me(bnserver) only, hence update it and return back to the user
								dataChunk.put(Integer.parseInt(deleteKey), null);
								System.out.println(deleteKey + " : " + dataChunk.get(Integer.parseInt(deleteKey)));
								System.out.println("Visited Servers: 0");
								System.out.println("Deletion Successful");
							} else {
								//Record for lookup is not with bnserver, hence delegate to further nodes for lookup
								//Open socket to next name server and pass the lookup responsibility
								Socket chExplorerSocket = null;
								PrintWriter chExplorerSocketOutPw = null;
								Scanner chExplorerSocketInSc = null;
								try {
									chExplorerSocket = new Socket(next.getIpAddress(),
											Integer.parseInt(next.getPortNumber()));
									chExplorerSocketOutPw = new PrintWriter(chExplorerSocket.getOutputStream(), true);
									chExplorerSocketInSc = new Scanner(chExplorerSocket.getInputStream());

									//Send the command to next name server in the chain
									chExplorerSocketOutPw.println(commandArr[0]);

									//Send the insert key to the next nameserver
									chExplorerSocketOutPw.println(deleteKey);

									//Print the response from the next node after exploring is done
									System.out.println(deleteKey + " : " + chExplorerSocketInSc.nextLine());
									List<String> visitedServers=Arrays.asList(
											(self.getNodeId() + "," + chExplorerSocketInSc.nextLine()).split(","));
									String serversVisited="";
									System.out.print("Visited Servers:");
									for(int i=0;i<visitedServers.size();i++ ) {
										if(i==visitedServers.size()-1) {
											System.out.println(visitedServers.get(i));
											
											System.out.println("The given key was successfully deleted at server "+visitedServers.get(i));
											
											
											
										}else {
											System.out.print(visitedServers.get(i)+"->");
										}
										
									}
//									System.out.println("Node Trace: " + Arrays.asList(
//											(self.getNodeId() + "," + chExplorerSocketInSc.nextLine()).split(",")));

								} catch (Exception e) {
									e.printStackTrace();
								} finally {
									chExplorerSocketInSc.close();
									chExplorerSocketOutPw.close();
									chExplorerSocket.close();
								}
							}
						} else {
							System.out.println("Key not found");
						} 
					} else {
						System.out.println("Invalid Command....");
					}
					break;

				default:
					System.out.println("Invalid Command.....");
					break;
				
				}
				
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} finally {
			userInSc.close();
		}
	}
	
}