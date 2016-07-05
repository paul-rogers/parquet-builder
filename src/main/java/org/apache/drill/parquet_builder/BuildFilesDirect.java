package org.apache.drill.parquet_builder;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;

public class BuildFilesDirect
{
  private File destDir;

  public BuildFilesDirect( File destDir ) {
    this.destDir = destDir;
  }
  
  public void build( ) throws IOException {
    buildInt32( );
    buildInt32Int32( );
    buildInt2Date( );
    buildInt32Int16( );
  }

  /**
   * Builds a file using only the int32 storage type with no logical
   * type annotations.
   * 
   * @throws IOException
   */
  
  public void buildInt32() throws IOException {
    File outFile = new File( destDir, "int32.parquet" );
    String schemaText = "message int32Data { required int32 index; required int32 value; }";
    SimpleParquetWriter writer = new SimpleParquetWriter( outFile, schemaText );
    writer.write( new IntWritable( 1 ), new IntWritable( 0 ) );
    writer.write( new IntWritable( 2 ), new IntWritable( -1 ) );
    writer.write( new IntWritable( 3 ), new IntWritable( 1 ) );
    writer.write( new IntWritable( 4 ), new IntWritable( Integer.MIN_VALUE ) );
    writer.write( new IntWritable( 5 ), new IntWritable( Integer.MAX_VALUE ) );
    writer.close( );
  }

  /**
   * Builds a file using the int32 storage type plus the int_32 logical type.
   * This file is not valid in Drill.
   * 
   * @throws IOException
   */
  
  public void buildInt32Int32() throws IOException {
    File outFile = new File( destDir, "int_32.parquet" );
    String schemaText = "message int32Data { required int32 index; required int32 value (INT_32); }";
    SimpleParquetWriter writer = new SimpleParquetWriter( outFile, schemaText );
    writer.write( new IntWritable( 1 ), new IntWritable( 0 ) );
    writer.write( new IntWritable( 2 ), new IntWritable( -1 ) );
    writer.write( new IntWritable( 3 ), new IntWritable( 1 ) );
    writer.write( new IntWritable( 4 ), new IntWritable( Integer.MIN_VALUE ) );
    writer.write( new IntWritable( 5 ), new IntWritable( Integer.MAX_VALUE ) );
    writer.close( );
  }
  
  /**
   * Builds a file using the int32 storage type plus the DATE logical type.
   * Drill accepts the file, but the resulting date values are off by about
   * 10,000 years.
   * 
   * @throws IOException
   */

  public void buildInt2Date() throws IOException {
    System.out.println( destDir.toString() );
    File outFile = new File( destDir, "date.parquet" );
    String schemaText = "message test { required int32 index; required int32 value (DATE); required int32 raw; }";
    SimpleParquetWriter writer = new SimpleParquetWriter( outFile, schemaText );
    writer.write( new IntWritable( 1 ), new IntWritable( 0 ), new IntWritable( 0 ) );
    writer.write( new IntWritable( 2 ), new IntWritable( -1 ), new IntWritable( -1 ) );
    writer.write( new IntWritable( 3 ), new IntWritable( 1 ), new IntWritable( 1 ) );
    writer.write( new IntWritable( 4 ), new IntWritable( Integer.MIN_VALUE ), new IntWritable( Integer.MIN_VALUE ) );
    writer.write( new IntWritable( 5 ), new IntWritable( Integer.MAX_VALUE ), new IntWritable( Integer.MAX_VALUE ) );
    int today = (int) (System.currentTimeMillis() / (24L*60*60*1000) );
    System.out.println( "Today: " + today );
    System.out.println( "Zero: " + (new Date( 0 ) ) );
    writer.write( new IntWritable( 6 ), new IntWritable( today ), new IntWritable( today ) );
    writer.close( );
  }
  
  /**
   * Builds a file with the int32 storage type but int_16 logical type.
   * Drill does not accept the file.
   * 
   * @throws IOException
   */
  
  public void buildInt32Int16() throws IOException {
    File outFile = new File( destDir, "int_16.parquet" );
    String schemaText = "message int16Data { required int32 index; required int32 value (INT_16); }";
    SimpleParquetWriter writer = new SimpleParquetWriter( outFile, schemaText );
    writer.write( new IntWritable( 1 ), new IntWritable( 0 ) );
    writer.write( new IntWritable( 2 ), new IntWritable( -1 ) );
    writer.write( new IntWritable( 3 ), new IntWritable( 1 ) );
    writer.write( new IntWritable( 4 ), new IntWritable( Short.MIN_VALUE ) );
    writer.write( new IntWritable( 5 ), new IntWritable( Short.MAX_VALUE ) );
    writer.close( );
  }

}
