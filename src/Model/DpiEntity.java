package Model;

public abstract class DpiEntity {

  private String url;
  private String userAccount;
  private String protocolType;
  private String sourceIP;
  private String destinationIP;
  private String sourcePort;
  private String domainName;
  private String referer;
  private String userAgent;
  private String Cookie;
  private String accessTime;
  
  
  public String getUrl() {
    // TODO Auto-generated method stub
    return this.url;
  }
  public void setUrl(String url){
    this.url=url;
  }
}
