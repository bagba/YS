package UrlMatcher;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class OwnRule extends DaoRule{

  public OwnRule(Path regexPath, Configuration conf) throws IOException {
    super(regexPath, conf);
    // TODO Auto-generated constructor stub
  }

}
