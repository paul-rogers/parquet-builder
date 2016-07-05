package org.apache.drill.parquet_builder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.parquet.hadoop.ParquetWriter.Builder;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public class SimpleBuilder extends Builder<ArrayWritable,SimpleBuilder>
{
  MessageType schema;
  
  protected SimpleBuilder(Path file) {
    super(file);
  }
  
  protected SimpleBuilder setSchema( String schemaText ) {
    this.schema = MessageTypeParser.parseMessageType( schemaText );
    return this;
  }

  @Override
  protected SimpleBuilder self() {
    return this;
  }

  @Override
  protected WriteSupport<ArrayWritable> getWriteSupport(Configuration conf) {
    return new DataWritableWriteSupport () {
      @Override
      public WriteContext init(Configuration configuration) {
          if (configuration.get(DataWritableWriteSupport.PARQUET_HIVE_SCHEMA) == null) {
              configuration.set(DataWritableWriteSupport.PARQUET_HIVE_SCHEMA, schema.toString());
          }
          return super.init(configuration);
      }
    };
  }    
}