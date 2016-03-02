import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileReaderDemo {

  public static void main(String[] args) throws IOException {
    // TODO Auto-generated method stub
    String uri = args[0];
    Configuration conf = new Configuration();
    Path path = new Path(uri);
    SequenceFile.Reader reader = null;
    try{
      reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
      Writable key = (Writable)
          ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      Writable value = (Writable)
          ReflectionUtils.newInstance(reader.getValueClass(), conf);
      long position = reader.getPosition();
      while(reader.next(key, value)){
        String syncSeen = reader.syncSeen()? "*":"";
        System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
        position = reader.getPosition();
      }
      
      System.out.println("-------------------------------------------------------Random-------------------------------------------------------");
      //第一种：如果不是记录边界，reader.next会抛出异常
      position = 314;
      reader.seek(position);
      reader.next(key, value);
      System.out.printf("[%s]\t%s\t%s\n", position, key, value);
      //第二种：找到下一个同步点
      position = 322;
      reader.sync(position);
      position = reader.getPosition();
      reader.next(key, value);
      System.out.printf("[%s]\t%s\t%s\n", position, key, value);
    } finally {
      IOUtils.closeStream(reader);
    }

  }

}
