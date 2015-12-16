package Model;

public class Mobile_3G_LogEntity extends DpiEntity{

  
  
  private String userAccount;
  private String protocolType;
  private String sourceIP;
  private String destinationIP;
  private String sourcePort;
  private String domainName;
  private String url;
  private String referer;
  private String userAgent;
  private String Cookie;
  private String accessTime;
  private String mismdn;
  public String getMismdn() {
    // TODO Auto-generated method stub
    return this.mismdn;
  }
  public void setMismdn(String mismdn){
    this.mismdn=mismdn;
  }
  
}
