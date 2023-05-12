import java.util.ArrayList;

public class FileInfo {

  //filename
  private String fileName;

  private String fileStatus;

  //filesize in bytes
  private long fileSize;

  //arraylist of Dstores where the file is stored
  private ArrayList<Integer> dstoresPortsWhereStored;

  public FileInfo(String fileName, ArrayList<Integer> dstoresPortsWhereStored, long fileSize) {
    this.fileName = fileName;
    this.dstoresPortsWhereStored = dstoresPortsWhereStored;
    this.fileSize = fileSize;
  }

  public void addDStorePort(Integer dstorePort) {
    dstoresPortsWhereStored.add(dstorePort);
  }

  public void removeDStorePort(Integer dstorePort) {
    dstoresPortsWhereStored.remove(dstorePort);
  }

  public ArrayList<Integer> getDStoresPortsArraylist() {
    return dstoresPortsWhereStored;
  }

  public void clearDStoresPortsArraylist() {
    dstoresPortsWhereStored.clear();
  }

  public String getFileName() {
    return fileName;
  }

  public long getFileSize() {
    return fileSize;
  }

  public void setFileStatus(String fileStatus) {
    this.fileStatus = fileStatus;
  }

  public String getFileStatus() {
    return fileStatus;
  }

  public ArrayList<Integer> getDStoresPorts() {
    return dstoresPortsWhereStored;
  }

  public void removeAllDStoresPorts() {
    dstoresPortsWhereStored.clear();
  }


}
