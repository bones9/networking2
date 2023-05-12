public class Request {

  private ClientInfo clientInfo;

  private String operation;

  private String fileName;

  private String fileSize;

  public Request(ClientInfo clientInfo, String operation) {
    this.clientInfo = clientInfo;
    this.operation = operation;
  }

  public ClientInfo getClientInfo() {
    return clientInfo;
  }

public String getClientName() {
    return clientInfo.getClientName();
  }

  public String getOperation() {
    return operation;
  }

  public String getFileName() {
    String[] words = operation.split(" ");
    String secondWord = words[1];
    return secondWord;
  }

  public long getFileSize() {
    String[] words = operation.split(" ");
    String thirdWord = words[2];
    return Long.parseLong(thirdWord);
  }

  public String getOperationType() {
    String[] words = operation.split(" ");
    String firstWord = words[0];
    return firstWord;
  }


}
