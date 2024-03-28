package pamCoordinator;




import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.Date;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Date;
import java.util.Map;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.TreeMap;

 class ParticipantHandlerThread implements Runnable {

	private Socket socket;
	private String participantId;

	public ParticipantHandlerThread(Socket socket) {
		this.socket = socket;
	}

	@Override
	public void run() {

		try {
			OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();
            PrintWriter socketOutPw = new PrintWriter(out,true);
            Scanner socketInSc = new Scanner(in);
            String[] commandArr;
           
			while (true) { //SAFE POINT TO WAIT
				String line = socketInSc.nextLine();
                commandArr = line.split(" ", 2);
                
                switch (commandArr[0]) {
				
                	case AppConstants.REGISTER:
					if (AppUtil.checkPattern("^register ([0-9]{4})$", line)) {
						//Read the participant ID
						String participantId = socketInSc.nextLine();
						//Read the participant IP address from socket
						String participantIPAddress = socket.getInetAddress().getHostAddress();
						//Read the participant's thread-B (messaging) Port number from socket
						String messagingPortNum = commandArr[1];
						/**--Register the participant--*/
						if (Coordinator.participants.containsKey(participantId)) {
							socketOutPw.println("The participant ID already exists, try with a new ID\n");
						} else {

							for (Map.Entry<String, Participant> participant : Coordinator.participants.entrySet()) {
								if (Thread.currentThread().getName().equals(participant.getValue().getThreadId())) {
									socketOutPw.println("The participant is already registered with a different ID\n");
									break;
								}
							}

							this.participantId = participantId;
							Coordinator.participants.put(participantId,
									new Participant(participantId, participantIPAddress,
											Integer.parseInt(messagingPortNum), "Online", "Y",
											new Date(System.currentTimeMillis()), Thread.currentThread().getName()));
							socketOutPw.println("Done\n");
						} 
					} else {
						socketOutPw.println("Invalid Command..\n");
					}
					break;
				
                	case AppConstants.DEREGISTER:
					if (AppUtil.checkPattern("^deregister$", line)) {
						//Remove the participant from the multicast group
						if (Coordinator.participants.containsKey(this.participantId)) {
							Coordinator.participants.remove(this.participantId);
							socketOutPw.println("Done\n");
						} else {
							socketOutPw.println("This participant is not yet registered, register first to deregister\n");
						} 
					} else {
						socketOutPw.println("Invalid Command..\n");
					}
					break;
				
                	case AppConstants.DISCONNECT:
					if (AppUtil.checkPattern("^disconnect$", line)) {
						//Change the status of the participant from online to offline
						if (Coordinator.participants.containsKey(this.participantId)
								&& Coordinator.participants.get(this.participantId).getStatus().equals("Online")) {
							//Make the participant offline
							Coordinator.participants.get(this.participantId).setStatus("Offline");
							Coordinator.participants.get(this.participantId)
									.setLastUpdated(new Date(System.currentTimeMillis()));
							socketOutPw.println("Done\n");
						} else if (!Coordinator.participants.containsKey(this.participantId)) {
							socketOutPw.println("This participant is not registered\n");
						} else {
							socketOutPw.println("This participant is already disconnected\n");
						} 
					} else {
						socketOutPw.println("Invalid Command..\n");
					}
					break;	
					
                	case AppConstants.RECONNECT:
                	
					if (AppUtil.checkPattern("^reconnect ([0-9]{4})$", line)) {
						if (Coordinator.participants.containsKey(this.participantId)
								&& Coordinator.participants.get(this.participantId).getStatus().equals("Offline")) {
							Socket messagingSocket = null;
							PrintWriter messagingSocketOutPw = null;
							try {
								//Open the socket to the thread-B of this particular participant where this instance of co-ordinator thread is connected.
								messagingSocket = new Socket(socket.getInetAddress().getHostAddress(),
										Coordinator.participants.get(this.participantId).getMessagingPortNum());
								messagingSocketOutPw = new PrintWriter(messagingSocket.getOutputStream(), true);

								//Then send all the messages to the participant that where posted in the message queue after td (Threshold Time)
								TreeMap<Date, String> sortedMessageBuffer = new TreeMap<Date, String>(
										Coordinator.messageBuffer);

								SortedMap<Date, String> messagesAfterTd = sortedMessageBuffer.subMap(
										new Date(System.currentTimeMillis() - Coordinator.thresholdTd * 1000),
										new Date(System.currentTimeMillis()));
								if (messagesAfterTd.size() == 0) {
									messagingSocketOutPw.println("");
								}
								int iterationCount = 0;
								//Send all the messages over the participants messaging socket
								for (Map.Entry<Date, String> message : messagesAfterTd.entrySet()) {
									iterationCount++;
									if (iterationCount == messagesAfterTd.entrySet().size()) {
										messagingSocketOutPw.println(
												message.getKey().toString() + " --> " + message.getValue() + "\n");
									} else {
										messagingSocketOutPw
												.println(message.getKey().toString() + " --> " + message.getValue());
									}
								}
							} catch (Exception e) {
								e.printStackTrace();
							} finally {
								//Close the socket connection to the participant
								messagingSocketOutPw.close();
								messagingSocket.close();
							}
							//Finally change the status and update the message port of the participant in the participant table.
							Coordinator.participants.get(this.participantId).setStatus("Online");
							Coordinator.participants.get(this.participantId)
									.setLastUpdated(new Date(System.currentTimeMillis()));
							Coordinator.participants.get(this.participantId)
									.setMessagingPortNum(Integer.parseInt(commandArr[1]));

							socketOutPw.println("Done\n");
						} else if (!Coordinator.participants.containsKey(this.participantId)) {
							socketOutPw.println("This participant is not registered\n");
						} else {
							socketOutPw.println("This participant is already connected (online)\n");
						} 
					} else {
						socketOutPw.println("Invalid Command..\n");
					}
					break;
                		
                	case AppConstants.MSEND:
					if (commandArr.length >= 2 && commandArr[0].equals("msend") && AppUtil.checkPattern("^[^\\s].*$", commandArr[1])) {
						//Put the message into the message buffer (queue) only if participant is registered and online.
						String multiCastMsg = commandArr[1];
						if (Coordinator.participants.containsKey(this.participantId)
								&& Coordinator.participants.get(this.participantId).getStatus().equals("Online")) {
							synchronized (Coordinator.messageBuffer) {
								Coordinator.messageBuffer.put(new Date(System.currentTimeMillis()), "(ParticipantID-"+this.participantId+"): "+multiCastMsg);
								/**---- Do a notifyAll() call on the Coordinator.messagebuffer object to notify Messenger thread to deliver a new message to all online participants -----*/
								Coordinator.messageBuffer.notifyAll();
							}
							socketOutPw.println("Done\n");
						} else if (!Coordinator.participants.containsKey(this.participantId)) {
							socketOutPw.println("This participant is not registered\n");
						} else {
							socketOutPw.println("This participant is not connected (offline)\n");
						} 
					} else {
						socketOutPw.println("Invalid Command..\n");
					}
					break;
                		
                	default:
                		socketOutPw.println("Invalid Command..\n");
                		break;
				}
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	} //END OF THE THREAD JOB

}

 class Participant {
    
	private String participantId;
	
	private String ipAddress;
	
	private int messagingPortNum;
	
	private String status;
	
	private String registered;
	
	private Date lastUpdated;
	
	private String threadId;

	public String getParticipantId() {
		return participantId;
	}

	public void setParticipantId(String participantId) {
		this.participantId = participantId;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public int getMessagingPortNum() {
		return messagingPortNum;
	}

	public void setMessagingPortNum(int messagingPortNum) {
		this.messagingPortNum = messagingPortNum;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getRegistered() {
		return registered;
	}

	public void setRegistered(String registered) {
		this.registered = registered;
	}

	public Date getLastUpdated() {
		return lastUpdated;
	}

	public void setLastUpdated(Date lastUpdated) {
		this.lastUpdated = lastUpdated;
	}

	public String getThreadId() {
		return threadId;
	}

	public void setThreadId(String threadId) {
		this.threadId = threadId;
	}
	
	public Participant(String participantId, String ipAddress, int messagingPortNum, String status, String registered,
			Date lastUpdated, String threadId) {
		this.participantId = participantId;
		this.ipAddress = ipAddress;
		this.messagingPortNum = messagingPortNum;
		this.status = status;
		this.registered = registered;
		this.lastUpdated = lastUpdated;
		this.threadId = threadId;
	}

	public Participant() {
		
	}

	@Override
	public String toString() {
		return "Participant [participantId=" + participantId + ", ipAddress=" + ipAddress + ", messagingPortNum="
				+ messagingPortNum + ", status=" + status + ", registered=" + registered + ", lastUpdated="
				+ lastUpdated + ", threadId=" + threadId + "]";
	}
	
	
}

 class MessengerThread implements Runnable {

	@Override
	public void run() {
		
		/**--------Activities for messenger thread
		 * 1) Will wait for the notifiction of a new message
		 * 2) Once notified it will inter into a for loop 
		 *        a) will open the socket connection to current online participant's messaging port (Handled by threadB of each paricipant)
		 *        b) send the message
		 *        c) close the socket connection
		 *        d) go to next iteration of loop
		 * 3) Once message is sent, it will again call wait() operation on the messageBuffer
		 */
		Thread.currentThread().setName("Messenger-Thread");
		while (true) {
			//Calls a wait method on the Cordinator.messageBuffer object.
			try {
				synchronized (Coordinator.messageBuffer) {
					Coordinator.messageBuffer.wait();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			TreeMap<Date, String> sortedMessageBuffer = new TreeMap<Date, String>(Coordinator.messageBuffer);
			//Deliver the messages to all the online (connected) participants.
			for (Map.Entry<String, Participant> participant : Coordinator.participants.entrySet()) {
				if (participant.getValue().getStatus().equals("Online")) {
					// a) will open the socket connection to current online participant's messaging port (Handled by threadB of each paricipant)
					Socket messagingSocket = null;
					PrintWriter messagingSocketOutPw = null;
					try {
						//Open the socket to the thread-B of this particular participant.
						messagingSocket = new Socket(participant.getValue().getIpAddress(),
								participant.getValue().getMessagingPortNum());
						messagingSocketOutPw = new PrintWriter(messagingSocket.getOutputStream(), true);
						// send the message to this particular participant
						messagingSocketOutPw.println(sortedMessageBuffer.lastEntry().getKey().toString()+" --> "+sortedMessageBuffer.lastEntry().getValue()+"\n");
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						//Close the socket
						messagingSocketOutPw.close();
						try {
							messagingSocket.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

				}
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
	
	/*-- The Asynchronous Communication Commands --*/
	public static final String REGISTER = "register";
	public static final String DEREGISTER = "deregister";
	public static final String DISCONNECT = "disconnect";
	public static final String RECONNECT = "reconnect";
	public static final String MSEND = "msend";
	
	
}

public class Coordinator {

	public static Map<Date, String> messageBuffer;

	public static Map<String, Participant> participants;

	public static Properties appProperties;

	public static long thresholdTd;

	public static int coOrdinatorPort;

	static {
		messageBuffer = Collections.synchronizedMap(new HashMap<Date, String>());
		participants = Collections.synchronizedMap(new HashMap<String, Participant>());
	}

	public static void main(String[] args) throws IOException {

		if (args.length != 1) {
			System.err.println("Pass the config file name for co-ordinator execution as the command line argument only");
			return;
		}

		/** --------Load the properties file into the application-------- */
		appProperties = new Properties();
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

		/** -----Get the properties from Properties Object------ */
		coOrdinatorPort = Integer.parseInt(appProperties.getProperty("coordinator.port"));
		thresholdTd = Long.parseLong(appProperties.getProperty("coordinator.threshold"));

		/** -------Instantiate a messenger thread------- */
		MessengerThread messengerRunnable = new MessengerThread();
		Thread messengerThread = new Thread(messengerRunnable);
		messengerThread.start();

		/** -------Instantiate a handler threads------- */
		try (ServerSocket listener = new ServerSocket(coOrdinatorPort)) {
			System.out.println("The Co-Ordinator has started!!!");
			ExecutorService pool = Executors.newFixedThreadPool(10);
			while (true) {
				pool.execute(new ParticipantHandlerThread(listener.accept()));
			}
		}

	}

}