import java.io.ByteArrayOutputStream;
import java.io.File;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;

public class AvroParseDiffPattern {

  public static void main(String[] args) throws Exception{
    // TODO Auto-generated method stub
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(AvroParseDiffPattern.class.getResourceAsStream("Pair.avsc"));
    
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("left", new Utf8("Hello"));
    datum.put("right", new Utf8("Avro"));
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> byteWriter = new GenericDatumWriter<GenericRecord>(schema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    byteWriter.write(datum, encoder);
    encoder.flush();
    out.close();
    
    Schema.Parser parserMore = new Schema.Parser();
    Schema schemaMore = parserMore.parse(AvroParseDiffPattern.class.getResourceAsStream("PairMore.avsc"));
    //写到ByteArrayOutputStream中的数据没有写入模式，所以这里要指定原来的模式schema
    DatumReader<GenericRecord> readerMore = new 
        GenericDatumReader<GenericRecord>(schema, schemaMore);
    Decoder decoderMore = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
    GenericRecord record = readerMore.read(null, decoderMore);
    System.out.println("Record --> left: "+ record.get("left")
      +" right: "+ record.get("right")
      +" description: "+ record.get("description"));
    
    File file = new File("data.avro");
    DatumWriter<GenericRecord> fileWriter = new 
        GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new 
        DataFileWriter<GenericRecord>(fileWriter);
    dataFileWriter.create(schema, file);
    dataFileWriter.append(datum);
    dataFileWriter.close();
    
    Schema.Parser parserLess = new Schema.Parser();
    Schema schemaLess = parserLess.parse(AvroParseDiffPattern.class.getResourceAsStream("PairLess.avsc"));
    //写到file中的数据有写入模式，所以这里不需要写原来的模式，用null代替
    DatumReader<GenericRecord> readerLess = new 
        GenericDatumReader<GenericRecord>(null, schemaLess);
    DataFileReader<GenericRecord> dataFileReader = new 
        DataFileReader<GenericRecord>(file, readerLess);
    while(dataFileReader.hasNext()){
      record = dataFileReader.next();
      System.out.println("Record --> left: "+ record.get("left")
        +" right: "+ record.get("right")
        +" description: "+ record.get("description"));
    }
    dataFileReader.close();

  }

}
