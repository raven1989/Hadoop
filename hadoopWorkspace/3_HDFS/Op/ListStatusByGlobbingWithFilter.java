import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class ListStatusByGlobbingWithFilter {

  public static class RegexExcludePathFilter implements PathFilter {
    private final String regex;
    public RegexExcludePathFilter(String regex){
      this.regex = regex;
    }
    public boolean accept(Path path) {
      return !path.toString().matches(regex);
    }
  }
  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    String uriPattern = args[0];
    String excludeRegex = args[1];
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uriPattern), conf);
    
    FileStatus[] status = fs.globStatus(new Path(uriPattern), new RegexExcludePathFilter(excludeRegex));
    Path[] paths = FileUtil.stat2Paths(status);
    for(Path p : paths) {
      System.out.println(p);
    }

  }

}
