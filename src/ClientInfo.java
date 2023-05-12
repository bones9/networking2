import java.io.BufferedReader;
import java.io.PrintWriter;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ClientInfo {

  private Socket clientSocket;

  private int clientPortWithController;

  private String clientName;

  private BufferedReader inClientBufferedReader;

  private PrintWriter outClientPrintWriter;

  private HashMap<String, ArrayList<Integer>> requestFiles = new HashMap<String, ArrayList<Integer>>();

  public ClientInfo(Socket clientSocket, int clientPortWithController, BufferedReader inClientBufferedReader, PrintWriter outClientPrintWriter) {
    this.clientSocket = clientSocket;
    this.clientPortWithController = clientPortWithController;
    this.clientName = "Client" + clientPortWithController;
    this.inClientBufferedReader = inClientBufferedReader;
    this.outClientPrintWriter = outClientPrintWriter;
  }

  public Socket getClientSocket() {
    return clientSocket;
  }

  public int getClientPortWithController() {
    return clientPortWithController;
  }

  public String getClientName() {
    return clientName;
  }

  public BufferedReader getInClientBufferedReader() {
    return inClientBufferedReader;
  }

  public PrintWriter getOutClientPrintWriter() {
    return outClientPrintWriter;
  }

  public void addRequestFile(String fileName){
    requestFiles.put(fileName, new ArrayList<Integer>());
  }

  public void addDstorePortInfoToRequestFile(String fileName, Integer i){
    requestFiles.get(fileName).add(i);
  }

  public ArrayList<Integer> getDstoreInfoFromRequestFile(String fileName){
    return requestFiles.get(fileName);
  }

  public Boolean isThereRequestFile(String fileName){
    return requestFiles.containsKey(fileName);
  }




}
