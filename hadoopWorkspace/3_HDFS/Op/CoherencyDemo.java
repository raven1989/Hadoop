import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CoherencyDemo {

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    String uri = args[0];
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    
    Path path = new Path(uri);
    FSDataOutputStream out = fs.create(path);
    
    if( !fs.exists(path) ) {
      System.out.println(path.toString()+" : not exists.");
    }
    
    out.write("content".getBytes("UTF-8"));
    out.flush();
    
    if( !(fs.getFileStatus(path).getLen()==0L) ) {
      System.out.println(path.toString()+" : no content.");
    }
    
    out.hsync();
    
    if( fs.getFileStatus(path).getLen()!=0L ) {
      System.out.println(path.toString()+" : sync data to file.");
    }

  }

}
