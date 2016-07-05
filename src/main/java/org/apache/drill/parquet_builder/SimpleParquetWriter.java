package org.apache.drill.parquet_builder;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public class SimpleParquetWriter
{
  private ParquetWriter<ArrayWritable> writer;
  
  public SimpleParquetWriter( Configuration config, Path dest, String schemaText ) throws IOException {
    init( config, dest, schemaText );
  }
  
  private void init( Configuration config, Path dest, String schemaText ) throws IOException
  {
    final MessageType schema = MessageTypeParser.parseMessageType( schemaText );
    DataWritableWriteSupport.setSchema(schema, config);
    FileSystem fs = dest.getFileSystem(config);
    fs.delete(dest, true);
    SimpleBuilder builder = new SimpleBuilder( dest )
        .setSchema( schemaText )
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withRowGroupSize(256*1024*1024)
        .withPageSize(100*1024)
        .withConf(config);
    writer = builder.build();
  }
  
  public SimpleParquetWriter( File dest, String schemaText ) throws IOException {
    Configuration config = new Configuration();
    File outDir = dest.getAbsoluteFile();
    Path outDirPath = new Path(outDir.toURI());
    init( config, outDirPath, schemaText );
  }
  
  public SimpleParquetWriter( SimpleBuilder builder ) throws IOException {
    writer = builder.build( );
  }
  
  public void write( Writable ... values  ) throws IOException {
    ArrayWritable value = new ArrayWritable(Writable.class, values);
    writer.write(value);
  }
  
  public void close( ) throws IOException {
    writer.close();
  }
}
