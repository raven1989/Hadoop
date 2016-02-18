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
import org.apache.avro.util.Utf8;

public class AvroFile {

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(AvroFile.class.getResourceAsStream("Pair.avsc"));
    
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("left", new Utf8("LEFT"));
    datum.put("right", new Utf8("RIGHT"));
    
    File file = new File("data.avro");
    DatumWriter<GenericRecord> writer = new 
        GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new 
        DataFileWriter<GenericRecord>(writer);
    dataFileWriter.create(schema, file);
    dataFileWriter.append(datum);
    dataFileWriter.close();
    
    DatumReader<GenericRecord> reader = new 
        GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> dataFileReader = new 
        DataFileReader<GenericRecord>(file, reader);
    
    GenericRecord record = null;
    while(dataFileReader.hasNext()){
      record = dataFileReader.next(record);
      System.out.println("record : "+ record.toString());
    }
    dataFileReader.close();

  }

}
