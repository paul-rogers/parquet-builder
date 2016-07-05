package org.apache.drill.parquet_builder;

import java.io.File;
import java.io.IOException;

public class Main 
{
    public static void main( String[] args )
    {
      try {
        new Main( ).run( );
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    private void run() throws IOException {
      File destDir = new File( "/Users/progers/play/data" );
      BuildFilesDirect builder = new BuildFilesDirect( destDir );
      builder.buildInt32Uint8( );
      builder.buildInt32Uint16( );
      builder.buildInt32Uint32( );
    }
}
