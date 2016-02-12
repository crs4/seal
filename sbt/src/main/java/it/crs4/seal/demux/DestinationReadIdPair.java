
package it.crs4.seal.demux;


public class DestinationReadIdPair {

  protected String destination = null;
  protected String readId = null;

  public DestinationReadIdPair() { }


  public DestinationReadIdPair(String dest, String readId) {
    this.destination = dest;
    this.readId = readId;
  }

  public String getDestination() { return destination; }
  public String getReadId() { return readId; }

  public void setDestination(String dest) { destination = dest; }
  public void setReadId(String id) { readId = id; }
}
