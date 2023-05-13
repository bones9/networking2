import javax.print.attribute.standard.MediaSize;
import java.io.*;
import java.net.*;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Supplier;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Dstore {

  //The logger for the Controller
  private static final Logger logger = Logger.getLogger(Controller.class.getName());

  //The port number the Dstore listens on for connections from the client.
  private int port;

  //The port number of the Controller to connect to.
  private int cport;

  //The timeout in milliseconds for waiting for responses from other processes.
  private int timeout;

  //The local directory where the Dstore stores files.
  private String fileFolderName;

  //The data structure to keep track of the stored files.
  private Map<String, File> storedFiles;



  // The output stream to the Controller
  PrintWriter outController;

  // The input stream from the Controller
  BufferedReader inController;

  // The socket to connect to the Controller
  Socket controllerSocket = null;



  ArrayList<ClientInfoForDstore> clients = new ArrayList<ClientInfoForDstore>();

  ArrayList<OtherDstoreInfoForDstore> otherDstores = new ArrayList<OtherDstoreInfoForDstore>();


  String currentDirectory = Paths.get("").toAbsolutePath().toString();
  String separator = File.separator;


  public static void main (String[] args) {

    //set up the logger
    logger.setUseParentHandlers(false);
    ConsoleHandler consoleHandler = new ConsoleHandler();
    consoleHandler.setLevel(Level.ALL);
    logger.addHandler(consoleHandler);

    //read in the arguments
    int port = Integer.parseInt(args[0]);
    int cport = Integer.parseInt(args[1]);
    int timeout = Integer.parseInt(args[2]);
    String fileFolderName = args[3];
    //create a new Dstore with the command line arguments
    Dstore dstore = new Dstore(port, cport, timeout, fileFolderName);

    dstore.emptyFileFolder(fileFolderName);
    dstore.connectToController();
    dstore.listenForClient();
    logger.info("Dstore is listening for clients on port " + port);
    dstore.listenForMessagesFromController();
    dstore.listenForMessagesFromClients();
    dstore.listenForMessagesFromOtherDstores();

  }

  // Dstore constructor
  public Dstore(int port, int cport, int timeout, String fileFolderName) {
    this.port = port;
    this.cport = cport;
    this.timeout = timeout;
    this.storedFiles = new HashMap<>();
    this.fileFolderName = fileFolderName;



/*    sendToController();
    listenToController();*/
  }

  //This is like the TCPSender code
  // Method to connect to the Controller which uses cport not port, so it can talk to the controller
  public void connectToController(){
    try{ controllerSocket = new Socket(getIPv4Address(), cport);
      outController = new PrintWriter(controllerSocket.getOutputStream());
      outController.println(Protocol.JOIN_TOKEN + " " + port);
      outController.flush();
      inController = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
      System.out.println("connected to controller");
    }catch(Exception e){System.out.println("Class Dstore: error"+e);}
  }

  public void listenForMessagesFromController() {
    Thread listenerThread = new Thread(() -> {
      try {
        while (true) {
          String message = inController.readLine();
          logger.info("Message received from Controller: " + message);
          if (message != null) {
            respondToMessageFromController(message);
            logger.info("Message received from Controller: " + message);
          } else {
            // Handle the case where the connection with the Controller is lost
            logger.info("Lost connection with Controller");
            break;
          }
        }
      } catch (Exception e) {
        logger.warning("Error while listening for messages from Controller: " + e.getMessage());
      } finally {
        // Clean up resources (e.g. close sockets, etc.)
        try {
          if (inController != null) {
            inController.close();
          }
          if (outController != null) {
            outController.close();
          }
          if (controllerSocket != null) {
            controllerSocket.close();
          }
        } catch (IOException e) {
          logger.warning("Error while closing resources: " + e.getMessage());
        }
      }
    });
    listenerThread.start();
  }


  public void listenForClient() {
    Thread listenerThread = new Thread(() -> {
      try {
        ServerSocket ss = new ServerSocket(port);
        logger.info("helloo");
        for (;;) {
          try {
            logger.info ("Waiting for client to send request...");
            Socket clientSocket = ss.accept(); //accept a connection from a client
            logger.info("Client connected: " + clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort());
            BufferedReader inClient = new BufferedReader(new InputStreamReader(clientSocket.getInputStream())); //get the input stream from the socket to read messages from the client
            PrintWriter outClient = new PrintWriter(clientSocket.getOutputStream()); //get the output stream from the socket to send messages to the client

            String message = inClient.readLine(); //get the message from the client

            if (message.startsWith(Protocol.REBALANCE_STORE_TOKEN)){ //then it's actually not a client but another dstore
              logger.info("Actually it was another dstore connected with message " + message + " from " + clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort());
              OtherDstoreInfoForDstore otherDstore = new OtherDstoreInfoForDstore(clientSocket, outClient, inClient);
                addOtherDstoreToOtherDstoresList(otherDstore);

                respondToRebalanceStoreMessage(message, otherDstore);
            }

            ClientInfoForDstore client = new ClientInfoForDstore(clientSocket, outClient, inClient);
            addClientToClientsList(client);


            logger.info("Message received: " + message);

            respondToMessage(message, client);


          } catch (Exception e) {
            System.out.println("Class Controller: error " + e);
          }
        }
      } catch (Exception e) {
        System.out.println("Class Controller: error " + e);
      }
    });
    listenerThread.start();  //start the thread which will listen for connections from dstores
  }


  public void listenForMessagesFromClients() {
    Thread listenerThread = new Thread(() -> {
      while (true) {
        for (ClientInfoForDstore client : clients) {
          try {
            BufferedReader inClient = client.getInClient();
            if (inClient.ready()) {
              String message = inClient.readLine();
              if (message != null) {
                respondToMessage(message, client);
                logger.info("Message received from client " + client.getClientSocket().getInetAddress().getHostAddress() + ": " + message);
              } else {
                // Handle the case where the connection with the client is lost
                logger.info("Lost connection with client " + client.getClientSocket().getInetAddress().getHostAddress());
                clients.remove(client);
                break;
              }
            }
          } catch (Exception e) {
            logger.warning("Error while listening for messages from clients: " + e.getMessage());
          }
        }
      }
    });
    listenerThread.start();
  }

  public void listenForMessagesFromOtherDstores() {
    Thread listenerThread = new Thread(() -> {
      while (true) {
        for (OtherDstoreInfoForDstore client : otherDstores) {
          try {
            BufferedReader inClient = client.getInOtherDstore();
            if (inClient.ready()) {
              String message = inClient.readLine();
              if (message != null) {
                respondToRebalanceStoreMessage(message, client);
                logger.info("Message received from dstore " + client.getOtherDstoreSocket().getInetAddress().getHostAddress() + ": " + message);
              } else {
                // Handle the case where the connection with the client is lost
                logger.info("Lost connection with dstore " + client.getOtherDstoreSocket().getInetAddress().getHostAddress());
                clients.remove(client);
                break;
              }
            }
          } catch (Exception e) {
            logger.warning("Error while listening for messages from dstores: " + e.getMessage());
          }
        }
      }
    });
    listenerThread.start();
  }


  public void respondToMessage(String message, ClientInfoForDstore client){
    String[] messageArray = message.split(" ");
    if(messageArray[0].equals(Protocol.STORE_TOKEN)){
      store(messageArray[1], Long.parseLong(messageArray[2]), client);
    }
    if(messageArray[0].equals(Protocol.LOAD_DATA_TOKEN)){
      load(messageArray[1], client);
    }
    if(messageArray[0].equals(Protocol.REMOVE_TOKEN)){
      remove(messageArray[1]);
    }
  }

  public void respondToMessageFromController(String message){
    String[] messageArray = message.split(" ");
    if(messageArray[0].equals(Protocol.REMOVE_TOKEN)){
      remove(messageArray[1]);
    }
    if(messageArray[0].equals(Protocol.LIST_TOKEN)){
      list();
    }
    if(messageArray[0].equals(Protocol.REBALANCE_TOKEN)){
      logger.info("about to start");
      rebalance(message);
      logger.info("Rebalance message received from controller...we get here");
    }
  }

  public void load(String fileName, ClientInfoForDstore client){
    try {
      //get the file from the file folder
      File file = storedFiles.get(fileName);
      byte[] fileData = new byte[(int) file.length()];
      try {
        FileInputStream fileInputStream = new FileInputStream(file);
        fileInputStream.read(fileData);
        fileInputStream.close();
      } catch (IOException e) {
        e.printStackTrace();
      }

      //send the file to the client
      try {
        Socket clientSocket = client.getClientSocket();
        clientSocket.setSoTimeout(timeout); // set the timeout for the socket
        OutputStream clientOutput = clientSocket.getOutputStream();
        clientOutput.write(fileData);
        clientOutput.flush();
      } catch (IOException e) {
        e.printStackTrace();
      }
      logger.info("File " + fileName + " sent to client");
      logger.info(client.getClientSocket().toString());

    } catch (Exception e) {
      logger.info("Class Dstore: error " + e);
    }
  }

  public void store(String fileName, long fileSize, ClientInfoForDstore client) {

    try {
      // Set the timeout for the client socket
      client.getClientSocket().setSoTimeout(timeout);

      //send an ack to the client
      client.getOutClient().println(Protocol.ACK_TOKEN);
      client.getOutClient().flush();
      logger.info("Sent ACK to client");

      //get the file content from the client to store
      try {
        InputStream clientInput = client.getClientSocket().getInputStream();
        byte[] fileData = clientInput.readNBytes((int) fileSize);
        storeFile(fileName, fileData);
      } catch (SocketTimeoutException e) {
        logger.warning("Timeout occurred while receiving file from client");
        return;
      } catch (IOException e) {
        e.printStackTrace();
      }
      logger.info("File " + fileName + " stored");

      //send the controller a STORE_ACK message
      outController.println(Protocol.STORE_ACK_TOKEN + " " + fileName);
      outController.flush();
      logger.info("Sent STORE_ACK to controller");

    } catch (SocketException e) {
      e.printStackTrace();
    }
  }


  //stores the file in the newly created file folder
  public void storeFile(String fileName, byte[] fileData){
    try {
      File file = new File(currentDirectory + separator + fileFolderName + separator + fileName);
      FileOutputStream fileOutputStream = new FileOutputStream(file);
      fileOutputStream.write(fileData);
      fileOutputStream.close();
      storedFiles.put(fileName, file);
      logger.info("File " + fileName + " stored");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void removeFile (String fileName) {
    try {
      File file = storedFiles.get(fileName);
      file.delete();
      storedFiles.remove(fileName);
      logger.info("File " + fileName + " removed");
    } catch (Exception e) {
      logger.info("Class Dstore: error " + e);
    }
  }

  public void remove(String fileName){

    if (storedFiles.containsKey(fileName)) {
      removeFile(fileName);
      outController.println(Protocol.REMOVE_ACK_TOKEN + " " + fileName);
      outController.flush();
      logger.info("Sent REMOVE_ACK to controller");
    } else {
      outController.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName);
      outController.flush();
      logger.info("Sent ERROR_FILE_DOES_NOT_EXIST to controller");
    }

  }

  //sends LIST + space seperated filenames to the controller
  public void list(){
    String fileNames = "";
    for (String fileName : storedFiles.keySet()) {
      fileNames += fileName + " ";
    }
    outController.println(Protocol.LIST_TOKEN + " " + fileNames);
    outController.flush();
    logger.info("Sent file list to controller:" + fileNames);
  }


  public void rebalance(String message){

    // Initialize data structures to store the files to send and delete
    Map<String, List<Integer>> filesToSend = new HashMap<>();
    List<String> filesToDelete = new ArrayList<>();


    logger.info("message issssss:: " + message);
    if (message.replaceAll(" ", "").equals("REBALANCE00")){
    //  logger.info("Files to send: " + filesToSend);
     // logger.info("Files to delete: " + filesToDelete);
      logger.info("message was REBALANCE 0 0 so no files to send or delete");
      //send the controller a REBALANCE_COMPLETE message
      outController.println(Protocol.REBALANCE_COMPLETE_TOKEN);
      outController.flush();
      return;
    }
    logger.info("we never did  the if statement");







    //split the message into individual parts by spaces
    String[] parts = message.split(" ");
    //reverse the order of the parts array
    String[] reversedParts = new String[parts.length];
    for (int i = 0; i < parts.length; i++){
      reversedParts[i] = parts[parts.length - 1 - i];
    }
    //System.out.println("reversedParts is: " + Arrays.toString(reversedParts));

    //find the index of the first element in the reversedParts array that is a number
    int firstNumberIndex = 0;
    for (int i = 0; i < reversedParts.length; i++){
      if (reversedParts[i].matches("\\d+")){
        firstNumberIndex = i;
        break;
      }
    }

    //length of the array
    int lengthOfArray = reversedParts.length;
    int indexInNotReversedArray = lengthOfArray - firstNumberIndex - 1;

    //System.out.println("firstNonNumberIndex is: " + indexInNotReversedArray);

    //split the array into two parts from that index

    String[] arr1 = Arrays.copyOfRange(parts, 0, indexInNotReversedArray);
    String[] arr2 = Arrays.copyOfRange(parts, indexInNotReversedArray, parts.length);

    //     System.out.println("arr1 is: " + Arrays.toString(arr1));
    //    System.out.println("arr2 is: " + Arrays.toString(arr2));


    int numOfFilesToDelete = Integer.parseInt(arr2[0]);
    //add the files to delete from arr2 to the filesToDelete list
    for (int i = 0; i < numOfFilesToDelete; i++){
      filesToDelete.add(arr2[i + 1]);
    }
 //   System.out.println("filesToDelete is: " + filesToDelete);

    //delete the first element of arr1
    String[] arr1WithoutFirstTwoElements = Arrays.copyOfRange(arr1, 2, arr1.length);
//    System.out.println("arr1WithoutFirstTwoElements is: " + Arrays.toString(arr1WithoutFirstTwoElements));

//find the number of files to send
    int numOfFilesToSend = Integer.parseInt(arr1[1]);
  //  System.out.println("numOfFilesToSend is: " + numOfFilesToSend);

    //iterate over the arr1WithoutFirstElement array and extract the files to send and add them to the filesToSend map

    for (int i =0 ; i < numOfFilesToSend; i++){
      //get the file name
      String fileName = arr1WithoutFirstTwoElements[0];
      //add the file name to the filesToSend map
      filesToSend.put(fileName, new ArrayList<>());

      int numOfPeers = Integer.parseInt(arr1WithoutFirstTwoElements[1]);

      for (int j = 2; j < numOfPeers + 2; j++){
        filesToSend.get(fileName).add(Integer.parseInt(arr1WithoutFirstTwoElements[j]));
      }

      //remove form the arr1WithoutFirstTwoElements array the file name and the number of peers
      arr1WithoutFirstTwoElements = Arrays.copyOfRange(arr1WithoutFirstTwoElements, numOfPeers + 2, arr1WithoutFirstTwoElements.length);
    }


 //   System.out.println("filesToSend is: " + filesToSend);








    logger.info("Files to send: " + filesToSend);
    logger.info("Files to delete: " + filesToDelete);
    //send the dstores that need the files from filesToSend the REBALANCE_STORE message in the form "REBALANCE_STORE fileName fileSize"
    for (String fileName : filesToSend.keySet()) {
      for (int port : filesToSend.get(fileName)) {

        Socket toSendSocket = null;
        PrintWriter out = null;
        BufferedReader in = null;

        try{ toSendSocket = new Socket(getIPv4Address(), port);
          out = new PrintWriter(toSendSocket.getOutputStream());
          out.println(Protocol.REBALANCE_STORE_TOKEN + " " + fileName + " " + storedFiles.get(fileName).length());
          out.flush();
          logger.info("Sent REBALANCE_STORE to dstore on port " + port);
          in = new BufferedReader(new InputStreamReader(toSendSocket.getInputStream()));
        }catch(Exception e){System.out.println("Class Dstore: error"+e);}

        try {
          String ackMessage = in.readLine();
          logger.info("Received " + ackMessage + " from dstore on port " + port);
            if (ackMessage.equals(Protocol.ACK_TOKEN)) {
                //send the file to the other dstore
                try {
                FileInputStream fileInputStream = new FileInputStream(storedFiles.get(fileName));
                byte[] fileData = new byte[(int) storedFiles.get(fileName).length()];
                fileInputStream.read(fileData);
                fileInputStream.close();

                OutputStream outputStream = toSendSocket.getOutputStream();
                outputStream.write(fileData);
                outputStream.flush();
                  toSendSocket.close(); //close the socket after sending the file


                logger.info("Sent file " + fileName + " to dstore on port " + port + " with size " + fileData.length);
                } catch (IOException e) {
                e.printStackTrace();
                }
            }
        } catch (IOException e) {
          e.printStackTrace();
        }


      }
    }

    logger.info("Finished sending files to other dstores");

    //remove the specified files
    for (String fileName : filesToDelete) {
      removeFile(fileName);
    }

    //send the controller a REBALANCE_COMPLETE message
    outController.println(Protocol.REBALANCE_COMPLETE_TOKEN);
    outController.flush();

  }


  //sending an Ack and receiving the file from the other dstore
  public void respondToRebalanceStoreMessage(String message, OtherDstoreInfoForDstore otherDstore){
    //message is of the form "REBALANCE_STORE fileName fileSize"
    String[] parts = message.split(" ");

    //respond to the other dstore with a ACK message
    otherDstore.getOutOtherDstore().println(Protocol.ACK_TOKEN);
    otherDstore.getOutOtherDstore().flush();
    logger.info("Sent ACK to other dstore");


   // try { Thread.sleep(2000); } catch (InterruptedException e) { e.printStackTrace(); }

    //get the file content from the other dstore to store
    try {
      logger.info("Waiting to receive file from other dstore with name + " + parts[1] + " and size " + parts[2]);
      InputStream otherDstoreInput = otherDstore.getOtherDstoreSocket().getInputStream();
      logger.info("1");
      byte[] fileData = otherDstoreInput.readNBytes(Integer.parseInt(parts[2]));
      logger.info("2");
      storeFile(parts[1], fileData);
      logger.info("Received file from other dstore with name + " + parts[1] + " and size " + parts[2]);
    } catch (SocketTimeoutException e) {
      logger.warning("Timeout occurred while receiving file from dstore");
      return;
    } catch (IOException e) {
      e.printStackTrace();
    }
    logger.info("File " + parts[1] + " stored");


  }




/*  //setup a loop to listen to messages from the controller
  public void listenToController(){
    while(true){
      try{
        String message = inController.readLine();
        System.out.println("Class Dstore: message from controller: "+message);
      }catch(Exception e){System.out.println("Class Dstore: error"+e);}
    }
  }*/

/*  //send messages to controller every 10 seconds
  public void sendToController(){
    while(true){
      try{
        Thread.sleep(10000);
        outController.println("hello I am " + port);
        outController.flush();
        logger.info("sent message to controller");
      }catch(Exception e){System.out.println("Class Dstore: error"+e);}
    }
  }*/

//

  //get the IP address of the machine so can connect to the controller because Socket needs it
  public String getIPv4Address() {
    try {
      InetAddress address = InetAddress.getLocalHost();
      byte[] ip = address.getAddress();
      if (ip.length != 4) {
        throw new UnknownHostException("Unable to get IPv4 address");
      }
      return String.format("%d.%d.%d.%d", ip[0] & 0xff, ip[1] & 0xff, ip[2] & 0xff, ip[3] & 0xff);
    } catch (UnknownHostException e) {
      e.printStackTrace();
      return null;
    }
  }


  //empties the folders contents if it already exists otherwise creates a new folder
  public void emptyFileFolder(String folderName) {
    File folder = new File(folderName);
    if (folder.exists()) {
      deleteFolderContents(folder);
    }
    else {
      folder.mkdir();
    }
  }

  //deletes the contents of a folder
  private void deleteFolderContents(File file) {
    if (file.isDirectory()) {
      for (File subFile : file.listFiles()) {
        deleteFolderContents(subFile);
      }
    } else {
      file.delete();
    }
  }

  public void addClientToClientsList(ClientInfoForDstore client){
    clients.add(client);
  }

  public class ClientInfoForDstore {

    // The output stream to the client
    PrintWriter outClient;

    // The input stream from the client
    BufferedReader inClient;


    // The socket to connect to the client
    Socket clientSocket = null;

    public ClientInfoForDstore(Socket clientSocket, PrintWriter outClient,
        BufferedReader inClient) {
      this.clientSocket = clientSocket;
      this.outClient = outClient;
      this.inClient = inClient;

    }

    public Socket getClientSocket() {
      return clientSocket;
    }

    public PrintWriter getOutClient() {
      return outClient;
    }

    public BufferedReader getInClient() {
      return inClient;
    }



  }

  public void addOtherDstoreToOtherDstoresList(OtherDstoreInfoForDstore otherDstore){
    otherDstores.add(otherDstore);
  }

  public class OtherDstoreInfoForDstore {

    // The output stream to the client
    PrintWriter outOtherDstore;

    // The input stream from the client
    BufferedReader inOtherDstore;


    // The socket to connect to the client
    Socket otherDstoreSocket = null;

    public OtherDstoreInfoForDstore(Socket otherDstoreSocket, PrintWriter outOtherDstore,
                               BufferedReader inOtherDstore) {
      this.otherDstoreSocket = otherDstoreSocket;
      this.outOtherDstore = outOtherDstore;
      this.inOtherDstore = inOtherDstore;

    }

    public Socket getOtherDstoreSocket() {
      return otherDstoreSocket;
    }

    public PrintWriter getOutOtherDstore() {
      return outOtherDstore;
    }

    public BufferedReader getInOtherDstore() {
      return inOtherDstore;
    }



  }



}



