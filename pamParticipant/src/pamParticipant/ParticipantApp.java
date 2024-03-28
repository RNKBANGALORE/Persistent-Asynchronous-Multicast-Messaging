package pamParticipant;



import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Properties;
import java.util.Scanner;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

 class MessageReceiverThread implements Runnable {

	@Override
	public void run() {
		
		//This thread will do following activities in each while iteraion-
		/*
		 * 1) It will open the server socket on the threadBSocketPort mentioned in ParticipantApp.java
		 * 2) It will listen for the socket connection
		 * 3) Once received connection it will read the messages using AppUtil.getServerResponse() utility (Modified One to write in file instead of console)
		 * 4) It will close the socket connection and go into the second iteration
		 *
		 */
		Thread.currentThread().setName("Message-Receiver-Thread");
		while (true) {
			try (ServerSocket listener = new ServerSocket(ParticipantApp.threadBSocketPort)) {
				
				/*
				 * Listen for a socket connection from either the MessengerThread(for single
				 * realtime multicast msg) of coordinator or the ParticipantHandlerThread (for
				 * old multicast msgs) of the coordinator
				 */
				Socket messagingSocket = listener.accept();
				Scanner messagingSocketInSc = new Scanner(messagingSocket.getInputStream());
				
				
				AppUtil.getPrintWriterResponseInFile("", messagingSocketInSc);
				
				
				messagingSocketInSc.close();
				messagingSocket.close();
	
			} catch (IOException e) {		
				e.printStackTrace();
			}
	
		}
	} //END OF THE THREAD JOB
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
	
	public static String getPrintWriterResponseInFile(String line, Scanner socketInSc) {
        while (true) {
            line = socketInSc.nextLine();
            if (line.equals("") || line.equals("goodbye")) {
                return line;
            }
            
            //Write the message line to the file
            File file = new File(ParticipantApp.appProperties.getProperty("participant.logfile"));
            try {
				//FileOutputStream fos = new FileOutputStream(file);
				FileWriter fileWriter = new FileWriter(file, true);
				PrintWriter printWriter = new PrintWriter(fileWriter);
			    printWriter.println("\n"+line);
			    printWriter.close();
			    fileWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
    }
	
	public static boolean checkPattern(String inputPattern, String data) {
		Pattern pattern = Pattern.compile(inputPattern);		
		Matcher matcher = pattern.matcher(data);
		return matcher.matches();
	}
}
 class AppConstants {
	
	/*-- The console cursor for user inputs --*/
	public static final String PARTICIPANT_CURSOR = "participant> ";
	
	/*-- The Asynchronous Communication Commands --*/
	public static final String REGISTER = "register";
	public static final String DEREGISTER = "deregister";
	public static final String DISCONNECT = "disconnect";
	public static final String RECONNECT = "reconnect";
	public static final String MSEND = "msend";
	
	
}

public class ParticipantApp {
	
	public static int threadBSocketPort;
	
	public static int oldThreadBPort;
	
	public static String registration;
	
	public static Properties appProperties;
	
	public static void main(String[] args) throws IOException {
		
		if (args.length != 1) {
			System.err.println("Pass the config file name for participant execution as the command line argument only");
			return;
		}
		
		/** --------Load the properties file into the application-------- */
		appProperties = new Properties();
		registration = "Enabled";
		
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
		String coordinatorIp = appProperties.getProperty("participant.coordinatorIp");
		int coordinatorPort = Integer.parseInt(appProperties.getProperty("participant.coordinatorPort"));
		String messageLogFile = appProperties.getProperty("participant.logfile");
		
		Socket socket = null;
        PrintWriter socketOutPw = null;
        Scanner socketInSc = null;
        Scanner userInSc = null;
        
        try {
			//Open the socket connection to the coordinator port (mentioned in the config file)
        	socket = new Socket(coordinatorIp, coordinatorPort);
            socketOutPw = new PrintWriter(socket.getOutputStream(), true);
        	socketInSc = new Scanner(socket.getInputStream());
        	userInSc = new Scanner(System.in);
            String[] commandArr; // command holder
            String line = "";        	
            boolean flag = false;

			while (true) {
				System.out.print(AppConstants.PARTICIPANT_CURSOR);
                line = userInSc.nextLine();  // what we entered
                System.out.println("You entered " + line);
                commandArr = line.split(" ", 2);
				
				switch (commandArr[0]) {					
					case AppConstants.REGISTER:
						if (!AppUtil.checkPattern("^register ([0-9]{4})$", line)) {
							socketOutPw.println(line);
							line = AppUtil.getPrintWriterResponse(line, socketInSc);
							break;
						}
						if (registration.equals("Enabled")) {
							//Read the file for the updated participant ID.
							fis = new FileInputStream(propertyFile);
							appProperties.load(fis);
							//Initialize the variable threadBSocketPort 
							threadBSocketPort = Integer.parseInt(commandArr[1]);
							if (flag == true) {
								//Just open a self-socket and close it fast to self thread-B just to push it to the next while loop iteration.
								Socket messagingSocket = null;
								PrintWriter messagingSocketOutPw = null;
								try {
									//Open the socket to the thread-B of this particular participant.
									messagingSocket = new Socket("localhost", oldThreadBPort);
									messagingSocketOutPw = new PrintWriter(messagingSocket.getOutputStream(), true);
									// send the message to this particular participant
									messagingSocketOutPw.println("");
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
								oldThreadBPort = threadBSocketPort;
							}
							if (flag == false) {
								//Spawn a threadB as a message receiving thread which listens on a particular port given by user in register command.
								MessageReceiverThread messageReceiverRunnable = new MessageReceiverThread();
								Thread messageReceiverThread = new Thread(messageReceiverRunnable);
								messageReceiverThread.start();
								oldThreadBPort = threadBSocketPort;
								flag = true;
							}
							//Send the command to the coordinator over the socket.
							socketOutPw.println(line);
							//Send the participant ID to the coordinator
							socketOutPw.println(appProperties.get("participant.id"));
							//Read & Print the response from coordinator
							line = AppUtil.getPrintWriterResponseRegistration(line, socketInSc);
							if (line.equals("Done")) {
								registration = "Disabled";
							}
						} else {
							System.out.println("Coordinator Response: You are already registered, deregister first and try again");
						}
						break;
				
					case AppConstants.RECONNECT:
						if (!AppUtil.checkPattern("^reconnect ([0-9]{4})$", line)) {
							socketOutPw.println(line);
							line = AppUtil.getPrintWriterResponse(line, socketInSc);
							break;
						}
						//Set the new port of threadB provided by user in reconnect command
						threadBSocketPort = Integer.parseInt(commandArr[1]);
						//Send the reconnect command to the coordinator over the socket
						socketOutPw.println(line);
						//Read & Print the response from coordinator
						line = AppUtil.getPrintWriterResponse(line, socketInSc);		
						break;
						
					case AppConstants.DEREGISTER:
						//Send the command to the coordinator over the socket
						socketOutPw.println(line);
						//Read & Print the response from coordinator
						line = AppUtil.getPrintWriterResponseRegistration(line, socketInSc);
						if (line.equals("Done")) {
							registration = "Enabled";
						}
						break;
						
						
					default:
						//Send the command to the coordinator over the socket
						socketOutPw.println(line);
						//Read & Print the response from coordinator
						line = AppUtil.getPrintWriterResponse(line, socketInSc);
						break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			socketOutPw.close();
			socketInSc.close();
			userInSc.close();
			socket.close();
		}
	}
}