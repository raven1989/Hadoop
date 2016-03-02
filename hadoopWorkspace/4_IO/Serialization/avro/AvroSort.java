import java.io.ByteArrayOutputStream;

import org.apache.avro.Schema;
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

public class AvroSort {

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    Schema.Parser writerParser = new Schema.Parser();
    Schema writerSchema = writerParser.parse(AvroSort.class.getResourceAsStream("SortedPair.avsc"));
    
    GenericRecord datum0 = new GenericData.Record(writerSchema);
    datum0.put("left", new Utf8("Hello"));
    datum0.put("right", new Utf8("Hadoop"));
    GenericRecord datum1 = new GenericData.Record(writerSchema);
    datum1.put("left", new Utf8("Greetings"));
    datum1.put("right", new Utf8("Avro"));
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new 
        GenericDatumWriter<GenericRecord>(writerSchema );
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(datum0, encoder);
    writer.write(datum1, encoder);
    encoder.flush();
    out.close();
    
    Schema.Parser readerParser = new Schema.Parser();
    Schema readerSchema = readerParser.parse(AvroSort.class.getResourceAsStream("SwitchedPair.avsc"));
    DatumReader<GenericRecord> reader = new 
        GenericDatumReader<GenericRecord>(readerSchema);
    Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
    GenericRecord record = reader.read(null, decoder);
    System.out.println("Record --> right: "+record.get("right")+" left: "+record.get("left"));
    record = reader.read(null, decoder);
    System.out.println("Record --> right: "+record.get("right")+" left: "+record.get("left"));

  }

}
