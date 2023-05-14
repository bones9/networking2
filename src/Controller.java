import java.io.*;
import java.net.*;
import java.nio.Buffer;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Controller {

  //The logger for the Controller
  private static final Logger logger = Logger.getLogger(Controller.class.getName());


  //The port number the Controller listens on for client connections.
  private int cport;

  //The replication factor for the files
  private int R;

  //The timeout in milliseconds for waiting for responses from other processes.
  private int timeout;

  //The time in seconds between rebalance operations.
  private int rebalance_Period;

  //The data structure to keep track of the stored files. Thread safe
  private ConcurrentHashMap<String, FileInfo> index = new ConcurrentHashMap<>();

  //Arraylist of DstoreInfos
  private CopyOnWriteArrayList<DstoreInfo> dstoreInfos = new CopyOnWriteArrayList<>();

  //Arraylist of ClientInfos
  private CopyOnWriteArrayList<ClientInfo> clientInfos = new CopyOnWriteArrayList<>();

  //clients will add their requests here. The controller will take requests from here and process them
  private LinkedBlockingQueue<Request> requestQueue = new LinkedBlockingQueue<>();

  private AtomicInteger operationsInProgress = new AtomicInteger(0);

  public void incrementOperationsInProgress() {
      operationsInProgress.incrementAndGet();
  }

    public void decrementOperationsInProgress() {
        operationsInProgress.decrementAndGet();
    }

    private AtomicBoolean rebalanceInProgress = new AtomicBoolean(false);


  private  ScheduledExecutorService rebalanceExecutor;

  public void startRebalanceExecutor() {
      rebalanceExecutor = Executors.newSingleThreadScheduledExecutor();
      rebalanceExecutor.scheduleAtFixedRate(this::rebalance, rebalance_Period, rebalance_Period, TimeUnit.SECONDS);
  }

  public void stopRebalanceExecutor() {
      rebalanceExecutor.shutdown();
  }

  public static void main(String[] args) {

    //set up the logger
    logger.setUseParentHandlers(false);
    ConsoleHandler consoleHandler = new ConsoleHandler();
    consoleHandler.setLevel(Level.ALL);
    logger.addHandler(consoleHandler);
    //logger.setLevel(Level.ALL);

    //read in the arguments
    int cport = Integer.parseInt(args[0]);
    int R = Integer.parseInt(args[1]);
    int timeout = Integer.parseInt(args[2]);
    int rebalance_Period = Integer.parseInt(args[3]);

    //create a new controller with the command line arguments
    Controller controller = new Controller(cport, R, timeout, rebalance_Period);
    logger.info("Controller has started");

    // Start the listener for dstores or clients joining
    controller.startDstoreAndClientJoiningListener();

    //start listener to check for whether it can serve requests from the requestQueue
    controller.startProcessingRequests();

    //start the rebalance executor
    controller.startRebalanceExecutor();



  }

  //Controller constructor
  public Controller(int cport, int R, int timeout, int rebalance_Period) {
    this.cport = cport;
    this.R = R;
    this.timeout = timeout;
    this.rebalance_Period = rebalance_Period;
  }


  //this is like the TCPReceiver code
  //Open a server socket on the controller port and listen for connections. Deduce whether from dstore or client
  //This version happens in its own thread
  public void startDstoreAndClientJoiningListener() {
    Thread listenerThread = new Thread(() -> {
      try {
        ServerSocket ss = new ServerSocket(cport);
        for (;;) {
          try {
            Socket socket = ss.accept(); //accept a connection from a dstore
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream())); //get the input stream from the socket to read messages from the dstore
            PrintWriter out = new PrintWriter(socket.getOutputStream()); //get the output stream from the socket to send messages to the dstore
            String message = in.readLine(); //get the join message from the dstore
            logger.info("Message received: " + message);
            if (message != null) {
              if (message.startsWith("JOIN")) { //then its a dstore joining
                int portNum = Integer.parseInt(
                    message.substring(5));  //get the port number from the join message
                logger.info(
                    "This initial message was received from a dstore. Client should use this port to connect to this dstore: "
                        + portNum);
                DstoreInfo dstoreInfo = new DstoreInfo(socket, socket.getPort(), portNum, in,
                    out); //add relevent information to a dstoreInfo
                addDstoreInfo(dstoreInfo); //add the dstoreInfo to the arraylist of dstoreInfos
                if (dstoreInfos.size() > R) {  //only rebalance if there are more than R dstores
                  logger.info("A dstore has joined and there are more than R , there are " + dstoreInfos.size() + " dstores so rebalancing");
                  stopRebalanceExecutor(); //stop the rebalance executor
                  rebalance(); //a dstore has joined so rebalance
                  startRebalanceExecutor();  //start the rebalance executor again after rebalancing
                }
              } else { // a clint is joining
                logger.info("This initial message was received from a client and was a request");
                ClientInfo clientInfo = new ClientInfo(socket, socket.getPort(), in, out); //adding relevant info to ClientInfo object
                addClientInfo(clientInfo); //adding ClientInfo object to arraylist
                requestQueue.put(
                    new Request(clientInfo, message)); //add the request to the requestQueue
                logger.info("Request added to requestQueue");
              }
            } else {
              logger.info("Message was null, so is a client joining with no request");
              ClientInfo clientInfo = new ClientInfo(socket, socket.getPort(), in, out); //adding relevant info to ClientInfo object
              addClientInfo(clientInfo); //adding ClientInfo object to arraylist
            }
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


  //add a dstoreInfo to the arraylist of dstoreInfos
  public void addDstoreInfo(DstoreInfo dstoreInfo){
    dstoreInfos.add(dstoreInfo);
  }

  public void removeDstoreInfo(DstoreInfo dstoreInfo){
    dstoreInfos.remove(dstoreInfo);
  }

  //add a clientInfo to the arraylist of clientInfos
  public void addClientInfo(ClientInfo clientInfo){
    clientInfos.add(clientInfo);
  }

  //get the arraylist of dstoreInfos
  public CopyOnWriteArrayList<DstoreInfo> getAllDstoreInfos(){
    return dstoreInfos;
  }

  //get a specific dstoreInfo by dstore Name
  public DstoreInfo getSpecificDstoreInfo(String dstoreName){
    for(DstoreInfo dstoreInfo : dstoreInfos){
      logger.info("Dstore name supplied is: " + dstoreName + " and the dstore name in the dstoreInfo is: " + dstoreInfo.getDstoreName());
      if (dstoreInfo.getDstoreName().equals(dstoreName))
      {
        return dstoreInfo;
      }
    }
    return null;
  }


  //get a specific clientInfo by clientName
  public ClientInfo getSpecificClientInfo(String clientName){
    for(ClientInfo clientInfo : clientInfos){
      if (clientInfo.getClientName().equals(clientName))
      {
        return clientInfo;
      }
    }
    return null;
  }

  public void loadOperation(String fileName,ClientInfo clientInfo) {

  //  incrementOperationsInProgress();

    BufferedReader inClient = clientInfo.getInClientBufferedReader();
    PrintWriter outClient = clientInfo.getOutClientPrintWriter();

    //failure handling if the number of Dstores is less than R
    if (dstoreInfos.size() < R){
      outClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
      outClient.flush();
      logger.info(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN + " " + "Not enough Dstores to store the file");
      return;
    }

    //error handling if file does not exist in index
    if (!index.containsKey(fileName)){
      outClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
      outClient.flush();
      logger.info(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + "File does not exist in index");
      return;
    }

    long fileSize = index.get(fileName).getFileSize();


    if (clientInfo.isThereRequestFile(fileName)){
      //reset the triedDstorePorts in the clientInfo because this is a LOAD operation therefore the first attempt
      clientInfo.getDstoreInfoFromRequestFile(fileName).clear();
    }
    else {
      clientInfo.addRequestFile(fileName); //add the request file to the clientInfo
    }

    Integer dstorePortToTry = whereToLoadFrom(fileName, clientInfo);

    outClient.println(Protocol.LOAD_FROM_TOKEN + " " + dstorePortToTry +  " " + fileSize);
    outClient.flush();

   // decrementOperationsInProgress();

  }

  public void reloadOperation(String fileName, ClientInfo clientInfo) {

    long fileSize = index.get(fileName).getFileSize();


    BufferedReader inClient = clientInfo.getInClientBufferedReader();
    PrintWriter outClient = clientInfo.getOutClientPrintWriter();

    //not resetting the triedDstorePorts in the clientInfo because this is a RELOAD operation therefore not the first attempt

    Integer dstorePortToTry = whereToLoadFrom(fileName, clientInfo);

    if (dstorePortToTry == null) {              //the dstore port to try was null meaning all the dstores that have the file have been tried already
      outClient.println(Protocol.ERROR_LOAD_TOKEN);
      outClient.flush();
    } else {   //the dstore port to try was not null meaning there is a dstore that has the file that has not been tried yet. send it that one then add it to the triedDstorePorts
      outClient.println(Protocol.LOAD_FROM_TOKEN + " " + dstorePortToTry +  " " + fileSize);
      outClient.flush();
      clientInfo.addDstorePortInfoToRequestFile(fileName, dstorePortToTry);
    }

  }

  //this method returns the port number of the dstore that the client should try to get the file from
  public Integer whereToLoadFrom(String fileName, ClientInfo clientInfo) {
    //getting dstores which have this file
     ArrayList<Integer> dstorePortsWithFile = index.get(fileName).getDStoresPorts();  //these are the ports that have the file

    ArrayList<Integer>  dstorePortsTried = clientInfo.getDstoreInfoFromRequestFile(fileName); //these are the dstores that have been tried already

    //get a dstore port which is in dstorePortsWithFile but not in dstorePortsTried. If there is none, return null
    Integer dstorePortToTry = null;
    for (Integer port : dstorePortsWithFile) {
      if (!dstorePortsTried.contains(port)) {
        dstorePortToTry = port;
        break;
      }
    }

    //add this dstoreport to the dstorePortsTried in the clientInfo
    clientInfo.addDstorePortInfoToRequestFile(fileName, dstorePortToTry);


    return dstorePortToTry;

  }










  //parameters include the name of the file, the size of the file, the input stream from the client and the output stream to the client
  public void storeOperation(String fileName, long fileSize, BufferedReader inClient, PrintWriter outClient){

    incrementOperationsInProgress();

    //failure handling if the number of Dstores is less than R
    if (dstoreInfos.size() < R){
      outClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
      outClient.flush();
      logger.info(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN + " " + "Not enough Dstores to store the file");
      decrementOperationsInProgress();
      return;
    }

    //failure handling if the file already exists in the index
    if (index.containsKey(fileName)){
      outClient.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
      outClient.flush();
      logger.info(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN + " " + "File already exists in the index");
      decrementOperationsInProgress();
      return;
    }


    printAllInformationAboutIndex();
    ArrayList<Integer> dstorePorts;
    dstorePorts = getDstorePortsToStoreFile(R); //choosing the R dstores where to store the file
    FileInfo fileInfo = new FileInfo(fileName, dstorePorts, fileSize);
    fileInfo.setFileStatus("store in progress");
    index.put(fileName, fileInfo); //updating the index

    //Countdownlatch which will only send the STORE_COMPLETE message to the client when all R dstores have sent an ACK
    CountDownLatch latch = new CountDownLatch(dstorePorts.size());

    //send client the store_to message with the dstore ports to which the file will be stored
    String numberString = String.join(" ", dstorePorts.toString().replace("[", "").replace("]", "").replace(",", "").trim());
    outClient.println(Protocol.STORE_TO_TOKEN + " " + numberString);
    outClient.flush();
    logger.info(Protocol.STORE_TO_TOKEN + " " + numberString);

    //waiting for an ACK from each of the R dstores. When all ACKs are received, the STORE_COMPLETE message is sent to the client
    for (int dstorePort : dstorePorts) {
      DstoreInfo dstoreInfo = getSpecificDstoreInfo("Dstore" + String.valueOf(dstorePort));
      logger.info("got a dstoreinfo object");
      new Thread(() -> {
        try {
          String message = dstoreInfo.getInDstoreBufferedReader().readLine();
          if (message.equals(Protocol.STORE_ACK_TOKEN + " " + fileName)) {
            dstoreInfo.addFileNameToStoredFiles(fileName);
            logger.info("Message received from dstore: " + message);
            latch.countDown();
          }
        } catch (SocketException e) { //in the case where we lose connection to dstore, remove it
          logger.info("Dstore connection reset: " + e);
          removeDstoreInfo(dstoreInfo); //removing this dstore from the list of dstores as it has disconnected
        } catch (IOException e) {
          e.printStackTrace();
        }
      }).start();
    }

    // Create a ScheduledExecutorService with a single thread
    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    // Create a Runnable that will cancel the operation after the specified timeout
    Runnable cancelTask = () -> {
      logger.info("Operation timeout reached: " + timeout + " milliseconds");
    };

    // Schedule the cancellation task to run after the specified timeout
    executorService.schedule(cancelTask, timeout, TimeUnit.MILLISECONDS);

    try {
      boolean completed = latch.await(timeout, TimeUnit.MILLISECONDS);
      if (completed) {
        // If completed within the timeout, cancel the scheduled task and proceed with the operation
        executorService.shutdownNow();
        outClient.println(Protocol.STORE_COMPLETE_TOKEN);
        outClient.flush();
        fileInfo.setFileStatus("store complete");
        logger.info("Message sent to client: " + Protocol.STORE_COMPLETE_TOKEN);
      } else {
        // If not completed within the timeout, the operation will be canceled by the scheduled task
        logger.info("Failed to receive all ACKs before timeout");
        index.remove(fileName); //remove file from index
        //remove file from all dstoreinfos that had it added
   //     for (int dstorePort : dstorePorts) {
    //      DstoreInfo dstoreInfo = getSpecificDstoreInfo("Dstore" + String.valueOf(dstorePort));
   //       dstoreInfo.removeFileNameFromStoredFiles(fileName);
    //    }
        decrementOperationsInProgress();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    printAllInformationAboutIndex();

    decrementOperationsInProgress();
  }

  //chooses R dstores ports where to store the file based on the number of files already stored in each dstore. Choses R dstores with the least number of files stored
  public ArrayList<Integer> getDstorePortsToStoreFile(int R){
    ArrayList<Integer> dstorePorts = new ArrayList<>();
    List<DstoreInfo> sortedDstores = dstoreInfos.stream()
        .sorted(Comparator.comparingInt(DstoreInfo::getNumberOfStoredFiles))
        .collect(Collectors.toList());
    for (int i = 0; i < R; i++){
      dstorePorts.add(sortedDstores.get(i).getDstorePortForClient());
    }
    return dstorePorts;
  }




  public void removeOperation(String fileName, ClientInfo clientInfo) {

    incrementOperationsInProgress();

    PrintWriter outClient = clientInfo.getOutClientPrintWriter();

    //failure handling if the file does not exist in the index
    if (!index.containsKey(fileName)) {
      outClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
      outClient.flush();
      logger.info(
          Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + "File does not exist in the index");
      decrementOperationsInProgress();
      return;
    }

    //failure handling if the number of Dstores is less than R
    if (dstoreInfos.size() < R) {
      outClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
      outClient.flush();
      logger.info(
          Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN + " " + "Not enough Dstores to store the file");
      decrementOperationsInProgress();
      return;
    }

    //update the index to "remove in progress"
    index.get(fileName).setFileStatus("remove in progress");

    //get the dstores which have this file
    ArrayList<Integer> dstorePorts = index.get(fileName).getDStoresPorts();

    //Countdownlatch which will only send the REMOVE_COMPLETE message to the client when all R dstores have sent an ACK
    CountDownLatch latch = new CountDownLatch(dstorePorts.size());

    //send each of the R dstores the remove message
    for (int dstorePort : dstorePorts) {
      DstoreInfo dstoreInfo = getSpecificDstoreInfo("Dstore" + String.valueOf(dstorePort));
      PrintWriter outDstore = dstoreInfo.getOutDstorePrintWriter();
      outDstore.println(Protocol.REMOVE_TOKEN + " " + fileName);
      outDstore.flush();
      logger.info("Message sent to dstore: " + Protocol.REMOVE_TOKEN + " " + fileName);
    }

    // Create a ScheduledExecutorService with a single thread
    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    // Create a Runnable that will cancel the operation after the specified timeout
    Runnable cancelTask = () -> {
      logger.info("Operation timeout reached: " + timeout + " milliseconds");
    };

    // Schedule the cancellation task to run after the specified timeout
    executorService.schedule(cancelTask, timeout, TimeUnit.MILLISECONDS);

    //wait for a REMOVE_ACK filename from each of the R dstores. When all ACKs are received, the REMOVE_COMPLETE message is sent to the client
    for (int dstorePort : dstorePorts) {
      DstoreInfo dstoreInfo = getSpecificDstoreInfo("Dstore" + String.valueOf(dstorePort));
      logger.info("got a dstoreinfo object");
      new Thread(() -> {
        try {
          String message = dstoreInfo.getInDstoreBufferedReader().readLine();
          if (message.equals(Protocol.REMOVE_ACK_TOKEN + " " + fileName)) {
            dstoreInfo.removeFileNameFromStoredFiles(
                fileName);  //removing from the dstoreinfo object the name of the file that it has just removed
            logger.info("Message received from dstore: " + message);
            latch.countDown();
          }
        } catch (SocketException e) {
          logger.info("Dstore connection reset: " + e);
          removeDstoreInfo(dstoreInfo); //removing this dstore from the list of dstores as it has disconnected.
          //not counting down the latch here so status remains "remove in progress" and method will return because of timeout in the end
        } catch (IOException e) {
          e.printStackTrace();
        }
      }).start();
    }

    try {
      boolean completed = latch.await(timeout, TimeUnit.MILLISECONDS);
      if (completed) {
        // If completed within the timeout, cancel the scheduled task and proceed with the operation
        executorService.shutdownNow();
        logger.info("All ACKs received from dstores for REMOVE operation");

        //they are removed from the index only after all ACK's have been received
        //so even if some did succeed and some did not, the ports will be removed from the index only if all ACK's have been received
        index.get(fileName).removeAllDStoresPorts();

        index.get(fileName).setFileStatus("remove complete");
        index.remove(fileName); //removing the file from the index as it has been removed from all dstores that had it
        outClient.println(Protocol.REMOVE_COMPLETE_TOKEN);
        outClient.flush();
        logger.info("Message sent to client: " + Protocol.REMOVE_COMPLETE_TOKEN);
        printAllInformationAboutIndex();
      } else {
        // If not completed within the timeout, the operation will be canceled by the scheduled task
        logger.info("Failed to receive all ACKs before timeout");
        decrementOperationsInProgress();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    decrementOperationsInProgress();
  }





  public void listOperation(ClientInfo clientInfo){
    PrintWriter outClient = clientInfo.getOutClientPrintWriter();

    //failure handling if the number of Dstores is less than R
    if (dstoreInfos.size() < R){
      outClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
      outClient.flush();
      logger.info(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN + " " + "Not enough Dstores to store the file");
      return;
    }


    //creating a space seperated list of all the files in the index with status "store complete"
    String list = index.values().stream()
        .filter(fileInfo -> fileInfo.getFileStatus().equals("store complete"))
        .map(FileInfo::getFileName)
        .collect(Collectors.joining(" "));

    outClient.println(Protocol.LIST_TOKEN + " " + list);
    outClient.flush();

    //rebalance();

  }

  public void dstoreListOPeration(){

    //send each of the dstores the LIST message
    for (DstoreInfo dstoreInfo : dstoreInfos) {
      PrintWriter outDstore = dstoreInfo.getOutDstorePrintWriter();
      outDstore.println(Protocol.LIST_TOKEN);
      outDstore.flush();
      dstoreInfo.resetListedFiles(); //resetting the list of files that the dstore has listed
    }


    List<Thread> threads = new ArrayList<>(); // Create a list to store the threads

    //get a reply from all the dstores and add the filenames to the list of files in the dstoreinfo object that they've listed,
    //which is different to what we currently think they are storing
    for (DstoreInfo dstoreInfo : dstoreInfos) {
      Thread thread = new Thread(() -> {
        try {
          String message = dstoreInfo.getInDstoreBufferedReader().readLine();
          // logger.info("bro the message is" + message);
          if (message.startsWith(Protocol.LIST_TOKEN)) {
            String[] messageParts = message.split(" ");
            for (int i = 1; i < messageParts.length; i++) {
              dstoreInfo.addFileNameToListedFiles(messageParts[i]);
            }
            //logger.info(dstoreInfo.getDstoreName() + "before revision has listed files: " + dstoreInfo.getListedFiles());
            dstoreInfo.createCopyOfListedFiles(); //creating a copy of the list of files that the dstore has listed as the original one gets manipulated to have the files it should end up with
          }
        } catch (SocketException e) {
          logger.info("Dstore connection reset: " + e);
          removeDstoreInfo(dstoreInfo); //removing this dstore from the list of dstores as it has disconnected.
        } catch (IOException e) {
          e.printStackTrace();
        }
      });

      thread.start();
      threads.add(thread); // Add the thread to the list of threads
    }

// Wait for all threads to finish executing
    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    //once they've finished so we've got all the repleis start revising the allocation
    reviseAllocation();


  }

  //also should check here if files are evenly distributed
  public boolean checkIfAllFilesAreInRDstores(){

    double numberOfDstores = dstoreInfos.size();
    double numberOfFiles = index.size();
    logger.info("number of files is " + numberOfFiles + " and number of dstores is " + numberOfDstores + " and R is " + String.valueOf(R));
    double minNumberOfFilesPerDstore =  Math.floor((R * numberOfFiles) / numberOfDstores);
    double maxNumberOfFilesPerDstore =  Math.ceil( (R * numberOfFiles) / numberOfDstores);
    logger.info("min number of files per dstore is " + minNumberOfFilesPerDstore + " and max number of files per dstore is " + maxNumberOfFilesPerDstore);


    for (FileInfo fileInfo : index.values()) {
      int count = 0;
        for (DstoreInfo dstoreInfo : dstoreInfos) {
            if (dstoreInfo.getListedFiles().contains(fileInfo.getFileName())){
            count++;
            }

            //if dstore has more or less files than the min and max teturn false
            if (dstoreInfo.getListedFiles().size() > maxNumberOfFilesPerDstore || dstoreInfo.getListedFiles().size() < minNumberOfFilesPerDstore){
              logger.info("a dstore has more or less files than the min and max");
              logger.info("the min is " + minNumberOfFilesPerDstore + " and the max is " + maxNumberOfFilesPerDstore + " and the dstore has " + dstoreInfo.getListedFiles().size() + " files");
              return false;
            }
        }
        if (count != R){
            logger.info("a file is not in  R dstores");
          return false;
        }
    }
    return true;
  }

  public void reviseAllocation(){

    //at this point can add some checks to see whether what they've listed is the same and if it is then can skip this revision processes

    boolean shouldSkipRevision = false;
    if (checkIfAllFilesAreInRDstores()) {
      logger.info("All files are in the correct dstores");
      for (DstoreInfo dstoreInfo : dstoreInfos) {
        Collections.sort(dstoreInfo.getListedFiles());
        Collections.sort(dstoreInfo.getStoredFilesNames());

        if (!dstoreInfo.getListedFiles().equals(dstoreInfo.getStoredFilesNames())){
          shouldSkipRevision = false;
          break;
        } else {
          shouldSkipRevision = true;
        }
      }

      if (shouldSkipRevision) {
        logger.info("skipping revision");
        return;
      }
    }


/*
    try {
      Thread.sleep(1000); //sleeping for a second to make sure all the dstores have replied to the LIST message
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
*/
    //logging what files the dstores said they had
    for (DstoreInfo dstoreInfo : dstoreInfos) {
      logger.info(dstoreInfo.getDstoreName() + " has listed files before any revision: " + dstoreInfo.getListedFiles());
    }

    //get the list of files in the index
    ArrayList<String> filesInIndex = new ArrayList<>(index.keySet());

    //loop through the dstoreinfos and if they have a file that is not in the index, remove it from the dstoreinfo. and if they have a file with status remove in progress delete them too
    for (String fileInIndex : filesInIndex) {
      boolean fileInAnyDstore = false;
      boolean removeFileFromIndex = false;
      for (DstoreInfo dstoreInfo : dstoreInfos) {
        ArrayList<String> filesInDstore = dstoreInfo.getListedFiles();
        if (filesInDstore.contains(fileInIndex)) {
          fileInAnyDstore = true;
          //if they have a file with status remove in progress delete them too
          if (index.containsKey(fileInIndex) && index.get(fileInIndex).getFileStatus().equals("remove in progress")) {
            dstoreInfo.removeFileNameFromListedFiles(fileInIndex);
            removeFileFromIndex = true;
          }
        }
      }
      //if the index has a file which is not in any of the dstores, remove it from the index
      if (!fileInAnyDstore) {
        index.remove(fileInIndex);
      } else if (removeFileFromIndex) { // if file is removed from dstore due to "remove in progress" status, remove it from index and other dstores
        index.remove(fileInIndex);
        for (DstoreInfo dstoreInfo : dstoreInfos) {
          if (dstoreInfo.getListedFiles().contains(fileInIndex)) {
            dstoreInfo.removeFileNameFromListedFiles(fileInIndex);
          }
        }
      }
      // if file is found in dstore but not in index, remove it from dstore
      if (fileInAnyDstore && !index.containsKey(fileInIndex)) {
        for (DstoreInfo dstoreInfo : dstoreInfos) {
          if (dstoreInfo.getListedFiles().contains(fileInIndex)) {
            dstoreInfo.removeFileNameFromListedFiles(fileInIndex);
          }
        }
      }
    }

    //by this point should the index should be updated to have removed files with status remove in progress (and those files would be removed from all dstores) and files that were not in any dstores
    //and files that were in dstores but not in the index would be deleted from the dstores.
    //so now only files in the index with store complete status should be in the dstores

    //logging what files the dstores said they had
    for (DstoreInfo dstoreInfo : dstoreInfos) {
      logger.info(dstoreInfo.getDstoreName() + " has listed files after a 1st bit of revison: " + dstoreInfo.getListedFiles());
    }


// This code block checks if any of the files in the index are stored in less than R dstores.
// If so, it stores the file in more dstores in order of which dstores have the least files until the file is in R dstores.
    for (String fileInIndex : filesInIndex) { // loop through all files in the index
      FileInfo fileInfo = index.get(fileInIndex); // get the file's info from the index
      if ( (fileInfo != null) && fileInfo.getFileStatus().equals("store complete") ) { // only process files with status 'store complete'
        ArrayList<DstoreInfo> dstoresWithFile = new ArrayList<>(); // create an ArrayList to store the dstores that already have the file
        for (DstoreInfo dstoreInfo : dstoreInfos) { // loop through all dstores to find the ones that have the file
          if (dstoreInfo.getListedFiles().contains(fileInIndex)) { // if the dstore has the file, add it to the list of dstores that already have the file
            dstoresWithFile.add(dstoreInfo);
          }
        }
        if (dstoresWithFile.size() < R) { // if the number of dstores with the file is less than R, store the file in more dstores
          // calculate the number of dstores to store the file in
          int numberOfDstoresToStoreIn = R - dstoresWithFile.size();
          ArrayList<DstoreInfo> dstoresToStoreIn = new ArrayList<>(); // create an ArrayList to store the dstores to store the file in
          for (DstoreInfo dstoreInfo : dstoreInfos) { // loop through all dstores to find the ones to store the file in
            if (!dstoresWithFile.contains(dstoreInfo)) { // if the dstore does not have the file, add it to the list of dstores to store the file in
              dstoresToStoreIn.add(dstoreInfo);
            }
          }
          // sort the dstoresToStoreIn by the number of files they have
          dstoresToStoreIn.sort(Comparator.comparingInt(DstoreInfo::getNumberOfListedFiles));
          // add the file to the dstoresToStoreIn until the number of dstores with the file is R
          for (int i = 0; i < numberOfDstoresToStoreIn; i++) {
            DstoreInfo dstoreInfo = dstoresToStoreIn.get(i);
            dstoreInfo.addFileNameToListedFiles(fileInIndex);
          }
        }
      }
    }

    //logging what files the dstores said they had
    for (DstoreInfo dstoreInfo : dstoreInfos) {
      logger.info(dstoreInfo.getDstoreName() + " has listed files after 2nd revision: " + dstoreInfo.getListedFiles());
    }


    //now the index should be updated to have files that are stored in R dstores and the dstores should have the files in their listed files
    //need to check whether files are evenly stored among dstores now e.g. With N Dstores, replication factor R, and F files, each Dstore should store between floor(RF/N) and ceil(RF/N) files E.g., with N=7, R=3, F=10, each Dstore should store between 4 and 5 files
    //if not evenly stored, need to move files from dstores with more than ceil(RF/N) files to dstores with less than floor(RF/N) files

    //loop through all dstores and check if they have more than ceil(RF/N) files, if so move a file to a dstore with less than floor(RF/N) files which doesn't yet have the file
    double numberOfDstores = dstoreInfos.size();
    double numberOfFiles = index.size();
   // logger.info("number of files is " + numberOfFiles + " and number of dstores is " + numberOfDstores + " and R is " + String.valueOf(R));
    double minNumberOfFilesPerDstore =  Math.floor((R * numberOfFiles) / numberOfDstores);
    double maxNumberOfFilesPerDstore =  Math.ceil( (R * numberOfFiles) / numberOfDstores);
    //logger.info("min number of files per dstore is " + minNumberOfFilesPerDstore + " and max number of files per dstore is " + maxNumberOfFilesPerDstore);


    for (DstoreInfo dstoreInfo : dstoreInfos) {
      int numberOfFilesInDstore = dstoreInfo.getNumberOfListedFiles();
        logger.info("dstore " + dstoreInfo.getDstoreName() + " has " + numberOfFilesInDstore + " files");
      if (numberOfFilesInDstore > maxNumberOfFilesPerDstore) {
        double numberOfFilesToMove = numberOfFilesInDstore - maxNumberOfFilesPerDstore;
        logger.info("dstore " + dstoreInfo.getDstoreName() + " has " + numberOfFilesInDstore + " files and needs to move " + numberOfFilesToMove + " files");

        while (numberOfFilesToMove > 0) {
          for (int i = 0; i < numberOfFilesInDstore; i++) {
            logger.info("number of files to move is " + numberOfFilesToMove + " and i is " + i + " and number of files in dstore is " + numberOfFilesInDstore + " and dstore name is " + dstoreInfo.getDstoreName());


              String fileToMove = dstoreInfo.getListedFiles().get(i);
                logger.info("file to move is " + fileToMove);
              for (DstoreInfo dstoreInfo2 : dstoreInfos) {
                if (dstoreInfo2.isListedFilesEmpty() || (!dstoreInfo2.getListedFiles().contains(fileToMove) && (dstoreInfo2.getNumberOfListedFiles() < minNumberOfFilesPerDstore))) {
                    logger.info("dstore " + dstoreInfo2.getDstoreName() + " has " + dstoreInfo2.getNumberOfListedFiles() );
                  dstoreInfo.removeFileNameFromListedFiles(fileToMove);
                  dstoreInfo2.addFileNameToListedFiles(fileToMove);
                  logger.info("moved file " + fileToMove + " from " + dstoreInfo.getDstoreName() + " to " + dstoreInfo2.getDstoreName());
                  numberOfFilesToMove--;
                  break;
                }
              }

              logger.info("broke out...number of files to move is " + numberOfFilesToMove + " and i is " + i + " and number of files in dstore is " + numberOfFilesInDstore + " and dstore name is " + dstoreInfo.getDstoreName());
            if (numberOfFilesToMove <= 0) {
              break; // exit while loop if there are no more files to move
            }


          }
        }

      }
      logger.info("this bit was aight");


      dstoreInfo.clearFilesToSendInRebalance(); //seperate to what else is going on. clear the files to send in rebalance list for each dstore
    }

    //logging what files the dstores said they had
    for (DstoreInfo dstoreInfo : dstoreInfos) {
      logger.info(dstoreInfo.getDstoreName() + " has listed files after 2nd to FINAL revision: " + dstoreInfo.getListedFiles());
    }

    for (DstoreInfo dstoreInfo : dstoreInfos) {

      int numberOfFilesInDstore = dstoreInfo.getNumberOfListedFiles();

      //if a dstore has less than the min number of files, start adding files from those with the most files until it has at least the min number of files
      if (numberOfFilesInDstore < minNumberOfFilesPerDstore) {
        double numberOfFilesToAdd  = minNumberOfFilesPerDstore - numberOfFilesInDstore;


        while (numberOfFilesToAdd > 0) {
          //go through the dstores in order of most files to least files and if they have more than the number of min files then add a file to the dstore
            for (int i = dstoreInfos.size() - 1; i >= 0; i--) {
                DstoreInfo dstoreInfo2 = dstoreInfos.get(i);
                if (dstoreInfo2.getNumberOfListedFiles() > minNumberOfFilesPerDstore) {
                  //go through the files that dstore2 has and if there is one that dstore1 doesn't have then add it to dstore1
                    for (String fileToMove : dstoreInfo2.getListedFiles()) {
                        if (!dstoreInfo.getListedFiles().contains(fileToMove)) {
                        dstoreInfo2.removeFileNameFromListedFiles(fileToMove);
                        dstoreInfo.addFileNameToListedFiles(fileToMove);
                        numberOfFilesToAdd--;
                        break;
                        }
                    }
                }

              if (numberOfFilesToAdd <= 0) {
                break; // exit while loop if there are no more files to move
              }

            }

          if (numberOfFilesToAdd <= 0) {
            break; // exit while loop if there are no more files to move
          }
        }

      }

    }

    //logging what files the dstores said they had
    for (DstoreInfo dstoreInfo : dstoreInfos) {
      logger.info(dstoreInfo.getDstoreName() + " has listed files after PROPER FINAL revision: " + dstoreInfo.getListedFiles());
    }






    constructRebalanceMessages();

  }


  public void constructRebalanceMessages(){

    for (DstoreInfo dstoreInfo : dstoreInfos){
      ArrayList<String> finalFileList = dstoreInfo.getListedFiles();
      ArrayList<String> initialFileList = dstoreInfo.getListedFilesCopy();

      int port = dstoreInfo.getDstorePortForClient();

      // get the files in the initial list that are not in the final list
      ArrayList<String> filesNotInFinalList = new ArrayList<>(initialFileList);
      filesNotInFinalList.removeAll(finalFileList); //these are the files to remove from the dstore
      int numberOfFilesToRemove = filesNotInFinalList.size();
      //String which is the number of files to remove followed by a space seperated list of the files to remove
      StringBuilder sb = new StringBuilder();
      sb.append(numberOfFilesToRemove);
      for (String fileToRemove : filesNotInFinalList) {
        sb.append(" ");
        sb.append(fileToRemove);
      }
      String resultString = sb.toString();
      logger.info("Remove part of rebalance message for dstore " +  dstoreInfo.getDstoreName() + ": " + resultString);
      dstoreInfo.setRemovePartOfRebalanceMessage(resultString); //set the remove part of the rebalance message for the dstore

      // get the files in the final list that are not in the initial list
      ArrayList<String> filesNotInInitialList = new ArrayList<>(finalFileList);
      filesNotInInitialList.removeAll(initialFileList); //these are the files to add to the dstore
      logger.info("Files not in initial list for dstore so it needs" + dstoreInfo.getDstoreName() + ": " + filesNotInInitialList);

      //for each file not in the initial list, find a dstore that has the file and add it to the list of files to send to the dstore
      for (String fileToAdd : filesNotInInitialList){
        for (DstoreInfo dstoreInfo2 : dstoreInfos){
          if (dstoreInfo2.getListedFilesCopy().contains(fileToAdd)){
            String[] fileNameAndPortToSendTo = {fileToAdd, String.valueOf(port)};
            dstoreInfo2.addFileToSendInRebalance(fileNameAndPortToSendTo);
            break;
          }
        }
      }

    }

    for (DstoreInfo dstoreInfo : dstoreInfos){
      logger.info("Files to send in rebalance message for dstore!!! " + dstoreInfo.getDstoreName() + ": " + dstoreInfo.getFilesToSendInRebalanceString());
    }


    //now each dstore info knows has what files to remove and what files to send. need to construct the message

    for (DstoreInfo dstoreInfo : dstoreInfos){

      StringBuilder sb = new StringBuilder();
      sb.append(Protocol.REBALANCE_TOKEN);
      sb.append(" ");
      sb.append(dstoreInfo.getFilesToSendInRebalance().size());
      sb.append(" ");

      ArrayList<String> messagesToSend = new ArrayList<>();
      for (String[] fileInfo : dstoreInfo.getFilesToSendInRebalance()) {
        String fileName = fileInfo[0];
        ArrayList<String> ports = new ArrayList<>();
        for (String[] fileInfo2 : dstoreInfo.getFilesToSendInRebalance()) {
          if (fileInfo2[0].equals(fileName)) {
            ports.add(fileInfo2[1]);
          }
        }
        String message = fileName + " " + ports.size() + " " + String.join(" ", ports);
        messagesToSend.add(message);
      }

      String sendPartOfMessage = String.join(" ", messagesToSend);

      sb.append(sendPartOfMessage);

      sb.append(" ");

      sb.append(dstoreInfo.getRemovePartOfRebalanceMessage());

      String resultString = sb.toString();

        dstoreInfo.setRebalanceMessage(resultString);

     // logger.info("Rebalance message constructed: " + resultString);
    }

    //each dstoreinfo now should have the full rebalance message

    //getting the rebalance messages for each dstore
    for (DstoreInfo dstoreInfo : dstoreInfos) {
      logger.info(dstoreInfo.getDstoreName() + " has rebalance message: " + dstoreInfo.getRebalanceMessage());
    }


    sendRebalanceMessages();


  }

  public void sendRebalanceMessages(){

    ArrayList<DstoreInfo> dstoresWhereRebalanceSent = new ArrayList<>();

    for (DstoreInfo dstoreInfo : dstoreInfos){
      PrintWriter outDstore = dstoreInfo.getOutDstorePrintWriter();
   //   if (!dstoreInfo.getRebalanceMessage().equals("REBALANCE 0 0")){  // if the dstore has no files to send or remove then don't send a rebalance message

      if (dstoreInfo.getRebalanceMessage().replaceAll(" ", "").equals("REBALANCE00")){
        dstoreInfo.setRebalanceMessage("REBALANCE 0 0");  //just getting rid of the double space issue i think there is
      }

        outDstore.println(dstoreInfo.getRebalanceMessage());
        outDstore.flush();
        logger.info("message sent: " + dstoreInfo.getRebalanceMessage() + " to " + dstoreInfo.getDstorePortForClient());
        dstoresWhereRebalanceSent.add(dstoreInfo);
   //   }
    }

    //now need to wait for the dstores to send back their rebalance complete messages
    //once we have it we can set what files it stores to what it now has after the manipulation (listed) and can clear the rebalance parts
    for (DstoreInfo dstoreInfo : dstoresWhereRebalanceSent){
      BufferedReader inDstore = dstoreInfo.getInDstoreBufferedReader();
      try {
        String rebalanceCompleteMessage = inDstore.readLine();
        logger.info("Rebalance complete message received from dstore " + dstoreInfo.getDstoreName() + ": " + rebalanceCompleteMessage);
        if (rebalanceCompleteMessage != null && rebalanceCompleteMessage.equals(Protocol.REBALANCE_COMPLETE_TOKEN)){
          dstoreInfo.setStoredFilesToListedFiles();
          dstoreInfo.clearRebalanceParts();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      dstoreInfo.clearRebalanceParts(); //still clear the rebalance parts even if the rebalance complete message is not received
    }


  }


 //rebalance with timeout
 public void rebalance(){
   // Wait until all operations are completed
   while (operationsInProgress.get() > 0 || (rebalanceInProgress.get() == true)) {
     try {
       logger.info("Waiting for operations to complete before rebalance");
       Thread.sleep(10); // Wait for 10 milliseconds
     } catch (InterruptedException e) {
       // Ignore interrupts
     }
   }

   //if number of dstores is 0 then don't rebalance
    if (dstoreInfos.size() == 0){
      logger.info("No dstores to rebalance");
      return;
    }


   rebalanceInProgress.set(true);
   logger.info("Rebalance started");

   Timer timer = new Timer();
   timer.schedule(new TimerTask() {
     @Override
     public void run() {
       logger.info("Rebalance timed out");
       rebalanceInProgress.set(false);
       return; // timeout reached, return from the method
     }
   }, timeout);

   dstoreListOPeration(); //reviseAllocation and constructRebalanceMessages are called in here and it now sends them too

   //sendRebalanceMessages();

   //need to sync the index with what the dstores now have
   syncIndexWithDstoreInfos();
   printAllInformationAboutIndex();

    rebalanceInProgress.set(false);
   timer.cancel(); // cancel the timer after the rebalance is completed
    logger.info("Rebalance completed");
 }

  public void syncIndexWithDstoreInfos(){

    //for every file in the index clear the list of dstores that have it
    for (String fileName : index.keySet()){
      index.get(fileName).clearDStoresPortsArraylist();

      //loop through all the dstores and see if it has the file, if it does add the port
        for (DstoreInfo dstoreInfo : dstoreInfos){
            if (dstoreInfo.doesDstoreHaveFile(fileName)){
            index.get(fileName).addDStorePort(dstoreInfo.getDstorePortForClient());
            }
        }

    }



  }



  public void startProcessingRequests() {
    listenForClientRequests();  //constantly listening for new client requests so that it can add them to the queue
    Thread requestProcessorThread = new Thread(() -> {
      while (true) {
        try {
          // Take a request from the queue, blocking if the queue is empty
          logger.info("Waiting for request to be added to the queue");

          Request request = requestQueue.take();
          logger.info("Request received: " + request);


          switch (request.getOperationType()) {
            case Protocol.STORE_TOKEN:
              String fileName = request.getFileName();
              long fileSize = request.getFileSize();
              BufferedReader inClient = getSpecificClientInfo(request.getClientName()).getInClientBufferedReader();
              PrintWriter outClient = getSpecificClientInfo(request.getClientName()).getOutClientPrintWriter();
              logger.info("store operation started");
              storeOperation(fileName, fileSize, inClient, outClient);
              logger.info("store operation completed");
              break;

            case Protocol.LOAD_TOKEN:
              loadOperation(request.getFileName(),request.getClientInfo());

              break;

            case Protocol.RELOAD_TOKEN:
              reloadOperation(request.getFileName(),request.getClientInfo());
              break;

            case Protocol.REMOVE_TOKEN:
              removeOperation(request.getFileName(),request.getClientInfo());
              break;

            case Protocol.LIST_TOKEN:
              listOperation(request.getClientInfo());
              break;

            default:
              //
              break;
          }

        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
    requestProcessorThread.start();
  }





  /*
  //trying to serve requests concurrently
    public void startProcessingRequests() {
    listenForClientRequests(); // constantly listening for new client requests so that it can add them to the queue

    //Making a thread pool with a maximum of 10 concurrent threads
    ExecutorService requestExecutorService = Executors.newFixedThreadPool(10);

    Thread requestProcessorThread = new Thread(() -> {
      while (true) {
        try {
          // Take a request from the queue, blocking if the queue is empty
          logger.info("Waiting for request to be added to the queue");

          Request request = requestQueue.take();
          logger.info("Request received: " + request);

          // Submit the request to the ExecutorService
          requestExecutorService.submit(() -> {
            switch (request.getOperationType()) {
              case Protocol.STORE_TOKEN:
                String fileName = request.getFileName();
                long fileSize = request.getFileSize();
                BufferedReader inClient = getSpecificClientInfo(request.getClientName()).getInClientBufferedReader();
                PrintWriter outClient = getSpecificClientInfo(request.getClientName()).getOutClientPrintWriter();
                logger.info("store operation started");
                storeOperation(fileName, fileSize, inClient, outClient);
                logger.info("store operation completed");
                break;

              case Protocol.LOAD_TOKEN:
                loadOperation(request.getFileName(), request.getClientInfo());
                break;

              case Protocol.RELOAD_TOKEN:
                reloadOperation(request.getFileName(), request.getClientInfo());
                break;

              case Protocol.REMOVE_TOKEN:
                removeOperation(request.getFileName(), request.getClientInfo());
                break;

              case Protocol.LIST_TOKEN:
                listOperation(request.getClientInfo());
                break;

              default:
                // Do nothing
                break;
            }
          });

        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
    requestProcessorThread.start();
  } */

  //constantly listening for client requests in another thread from the list of clientInfos the creating new requests and adding them to the queue
  public void listenForClientRequests(){
    Thread requestListenerThread = new Thread(() -> {
      while (true) {
        try {
          for (ClientInfo clientInfo : clientInfos) {
            if (clientInfo.getInClientBufferedReader().ready()) {
              String message = clientInfo.getInClientBufferedReader().readLine();
              logger.info("Message received from client: " + message);
              requestQueue.put(new Request(clientInfo, message)); // add the request to the requestQueue
              logger.info("Request added to requestQueue");
            }
          }
        } catch (IOException | InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
    requestListenerThread.start();
  }


  public void printAllInformationAboutIndex(){
    for (Map.Entry<String, FileInfo> entry : index.entrySet()){
      System.out.println("File name: " + entry.getKey() + ", file info: " + entry.getValue());
      System.out.println("File status: " + entry.getValue().getFileStatus() + ", file size: " + entry.getValue().getFileSize() + ", file dstore ports: " + entry.getValue().getDStoresPorts());

    }
  }





}




