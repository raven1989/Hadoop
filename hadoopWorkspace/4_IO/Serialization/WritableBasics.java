import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.StringUtils;

public class WritableBasics {
  
  public static byte[] serialize(Writable writable) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    //1.Interface Writable的第一个接口
    writable.write(dataOut);
    dataOut.close();
    return out.toByteArray();
  }
  
  public static byte[] deserialize(Writable writable, byte[] bytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    DataInputStream dataIn = new DataInputStream(in);
    //2.Interface Writable的第二个接口
    writable.readFields(dataIn);
    dataIn.close();
    return bytes;
  }

  public static void main(String[] args) throws IOException {
    // TODO Auto-generated method stub
    IntWritable writable = new IntWritable();
    writable.set(163);
    
    byte[] bytes = serialize(writable);
    String bytes4show = StringUtils.byteToHexString(bytes);
    System.out.println("Serialized bytes: "+bytes4show+" length: "+bytes.length);
    
    IntWritable newWritable = new IntWritable();
    deserialize(newWritable, bytes);
    System.out.println("Deserialized: "+newWritable.get());
    
    @SuppressWarnings("unchecked")
    RawComparator<IntWritable> comparator = WritableComparator.get(IntWritable.class);
    IntWritable w1 = new IntWritable(163);
    IntWritable w2 = new IntWritable(67);
    System.out.println("Compare "+w1.get()+" to "+w2.get()+": "+comparator.compare(w1, w2) );
    byte[] b1 = serialize(w1);
    byte[] b2 = serialize(w2);
    System.out.println("Compare "+StringUtils.byteToHexString(b1)+" to "+
      StringUtils.byteToHexString(b2)+": "+
      //使用RawComparator的比较接口，这个接口直接比较，不做反序列化
      comparator.compare(b1, 0, b1.length, b2, 0, b2.length) );
    
    Text text = new Text("hadoop");
    System.out.println("Text: "+text.toString()+" length: "+text.getLength()
      +"byte length: "+text.getBytes().length+" charAt(2):"+(char)(text.charAt(2)) );
    
    String s = "\u0041\u00DF\u6771\uD801\uDC00";
    Text t = new Text("\u0041\u00DF\u6771\uD801\uDC00");
    System.out.println("                        String                               Text");
    System.out.println("content                 "+s+"                               "+t.toString());
    System.out.println("length                  "+s.length()+"                                   "+t.getLength());
    System.out.println("bytes length            "+s.getBytes("UTF-8").length+"                                   \\");
    System.out.println("indexOf(\"\\u0041\")       "+s.indexOf("\u0041")
      +"                                   "+t.find("\u0041"));
    System.out.println("indexOf(\"\\u00DF\")       "+s.indexOf("\u00DF")
      +"                                   "+t.find("\u00DF"));
    System.out.println("indexOf(\"\\u6771\")       "+s.indexOf("\u6771")
      +"                                   "+t.find("\u6771"));
    System.out.println("indexOf(\"\\uD801\\uDC00\") "+s.indexOf("\uD801\uDC00")
      +"                                   "+t.find("\uD801\uDC00"));//字符代理对, surrogate pair
    System.out.println("charAt(0)               "+"0x"+Integer.toHexString(s.charAt(0))
    +"                                 "+"0x"+Integer.toHexString(t.charAt(0)) );
    System.out.println("charAt(1)               "+"0x"+Integer.toHexString(s.charAt(1))
    +"                                 "+"0x"+Integer.toHexString(t.charAt(1)) );
    System.out.println("charAt(2)               "+"0x"+Integer.toHexString(s.charAt(2))
    +"                               "+"0x"+Integer.toHexString(t.charAt(2)) );
    System.out.println("charAt(3)               "+"0x"+Integer.toHexString(s.charAt(3))
    +"                               "+"0x"+Integer.toHexString(t.charAt(3)) );
    System.out.println("charAt(4)               "+"0x"+Integer.toHexString(s.charAt(4))
    +"                               "+"0x"+Integer.toHexString(t.charAt(4)) );
    System.out.println("charAt(5)               "+"\\"
    +"                                   "+"0x"+Integer.toHexString(t.charAt(5)) );
    System.out.println("charAt(6)               "+"\\"
    +"                                   "+"0x"+Integer.toHexString(t.charAt(6)) );
    System.out.println("charAt(7)               "+"\\"
    +"                                   "+"0x"+Integer.toHexString(t.charAt(7)) );
    System.out.println("charAt(8)               "+"\\"
    +"                                   "+"0x"+Integer.toHexString(t.charAt(8)) );
    System.out.println("charAt(9)               "+"\\"
    +"                                   "+"0x"+Integer.toHexString(t.charAt(9)) );
    System.out.println("codePointAt(0)          "+"0x"+Integer.toHexString(s.codePointAt(0))
    +"                                 "+"\\" );
    System.out.println("codePointAt(1)          "+"0x"+Integer.toHexString(s.codePointAt(1))
    +"                                 "+"\\" );
    System.out.println("codePointAt(2)          "+"0x"+Integer.toHexString(s.codePointAt(2))
    +"                               "+"\\" );
    System.out.println("codePointAt(3)          "+"0x"+Integer.toHexString(s.codePointAt(3))
    +"                              "+"\\" );
    
  }

}
