import java.io.BufferedReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;


//Information about dtstores that have connected to the controller
public class DstoreInfo {

  //the socket the dstore is connected on
  private Socket dstoreSocket;

  //the port the dstore is connected on
  private int dstorePortWithController;

  //the port the client can use to connect to the dstore
  private int dstorePortForClient;

  private String dstoreName;

  //the input stream of the dstore
  private BufferedReader inDstoreBufferedReader;

  //the output stream of the dstore
  private PrintWriter outDstorePrintWriter;

  private ArrayList<String> storedFiles = new ArrayList<String>();

  private ArrayList<String> listedFiles = new ArrayList<String>();
//
  private ArrayList<String> listedFilesCopy = new ArrayList<String>();

  private String removePartOfRebalanceMessage = "";

  private ArrayList<String[]> filesToSendInRebalance = new ArrayList<String[]>();

  private String rebalanceMessage = "";
  public DstoreInfo(Socket dstoreSocket, int dstorePortWithController, int dstorePortForClient, BufferedReader inDstoreBufferedReader, PrintWriter outDstorePrintWriter) {
    this.dstoreSocket = dstoreSocket;
    this.dstorePortWithController = dstorePortWithController;
    this.dstorePortForClient = dstorePortForClient;
    this.dstoreName = "Dstore" + dstorePortForClient;
    this.inDstoreBufferedReader = inDstoreBufferedReader;
    this.outDstorePrintWriter = outDstorePrintWriter;
  }

  public int getDstorePortWithController() {
    return dstorePortWithController;
  }

  public BufferedReader getInDstoreBufferedReader() {
    return inDstoreBufferedReader;
  }

  public PrintWriter getOutDstorePrintWriter() {
    return outDstorePrintWriter;
  }

  public Socket getDstoreSocket() {
    return dstoreSocket;
  }

  public int getDstorePortForClient() {
    return dstorePortForClient;
  }

  public String getDstoreName() {
    return dstoreName;
  }

  public ArrayList<String> getStoredFilesNames() {
    return storedFiles;
  }

  public void addFileNameToStoredFiles(String fileName) {
    storedFiles.add(fileName);
  }

  public void removeFileNameFromStoredFiles(String fileName) {
    if (storedFiles.contains(fileName)){
      storedFiles.remove(fileName);
    }
  }
  public int getNumberOfStoredFiles() {
    if (storedFiles == null) {
      return 0;
    } else {
      return storedFiles.size();
    }
  }

  public Socket getSocket() {
    return dstoreSocket;
  }

  public void addFileNameToListedFiles(String fileName) {
    listedFiles.add(fileName);
  }

  public void resetListedFiles() {
    listedFiles.clear();
  }

  public ArrayList<String> getListedFiles() {
    return listedFiles;
  }

  public int getNumberOfListedFiles() {
    if (listedFiles == null) {
      return 0;
    } else {
      return listedFiles.size();
    }
  }

  public void removeFileNameFromListedFiles(String fileName) {
    if (listedFiles.contains(fileName)){
      listedFiles.remove(fileName);
    }
  }

  public void createCopyOfListedFiles(){
    listedFilesCopy.clear();
    listedFilesCopy = new ArrayList<String>(listedFiles);
  }

  public ArrayList<String> getListedFilesCopy(){
    return listedFilesCopy;
  }

  public void setRemovePartOfRebalanceMessage(String message){
    removePartOfRebalanceMessage = message;
  }

  public String getRemovePartOfRebalanceMessage(){
    return removePartOfRebalanceMessage;
  }

  public void addFileToSendInRebalance(String[] fileNameAndPortToSendTo){
    filesToSendInRebalance.add(fileNameAndPortToSendTo);
  }

  public ArrayList<String[]> getFilesToSendInRebalance(){
    return filesToSendInRebalance;
  }




    //method which constructs a string of all the files   that are to be sent in rebalance
    public String getFilesToSendInRebalanceString(){
        String filesToSendInRebalanceString = "";
        for (String[] fileNameAndPortToSendTo : filesToSendInRebalance){
        filesToSendInRebalanceString = filesToSendInRebalanceString + fileNameAndPortToSendTo[0] + " " + fileNameAndPortToSendTo[1] + "\n";
        }
        return filesToSendInRebalanceString;
    }

  public void clearFilesToSendInRebalance(){
    filesToSendInRebalance.clear();
  }

  public void setRebalanceMessage(String message){
    rebalanceMessage = message;
  }

    public String getRebalanceMessage(){
        return rebalanceMessage;
    }


    public void clearRebalanceParts(){
    clearFilesToSendInRebalance();
setRebalanceMessage("");
setRemovePartOfRebalanceMessage("");

//set listed files to empty
        listedFiles.clear();
        //set listed files copy to empty
        listedFilesCopy.clear();
    }

    public void setStoredFilesToListedFiles(){
    storedFiles.clear();
    storedFiles = new ArrayList<String>(listedFiles);
    }

    public boolean doesDstoreHaveFile(String fileName){
    return storedFiles.contains(fileName);
    }

    public boolean isListedFilesEmpty(){
    return listedFiles.isEmpty();
    }


}
