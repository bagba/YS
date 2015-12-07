package UrlMatcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public abstract class DaoRule {

  private HashMap<String, String> rules;
  private Configuration conf;
  private Path regexPath;
  private String matched;
  private FileSystem fileSystem = null;
  private BufferedReader bufferReader = null;
 

  DaoRule(Path regexPath, Configuration conf) throws IOException {
    this.regexPath = regexPath;
    this.conf = conf;
    if (load(this.regexPath, this.conf)) {
      System.out.print("load ok!");
    };
  }

  private boolean load(Path regexPath2, Configuration conf2) {
    // TODO Auto-generated method stub
    this.rules=new HashMap<>();
    try {
      fileSystem=FileSystem.get(conf2);
      this.bufferReader = 
          new BufferedReader(
          new InputStreamReader(fileSystem.open(regexPath2)));
      String line = "";
      while ((line = this.bufferReader.readLine()) != null) {
        String[] splits = StringUtils.split(line, ' ');
        if (splits.length == 2)
          this.rules.put(splits[0], splits[1]);
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return false;
    }
   
    return true;
  }

  public boolean matches(String input) {
    Pattern pattern ;
    Matcher matcher;
    for(Map.Entry<String, String>entry:rules.entrySet()){
      pattern=Pattern.compile((String)entry.getValue());  
      matcher = pattern.matcher(input);
      if(matcher.matches()){
        matched=entry.getKey();
        return true;
      }
    }
    return false;
  };
  
  public String getMatched(){
    return matched;
  }
}
