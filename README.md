# Parquet Test File Builder

This little project contains code to build Parquet files for testing. The purpose is to try odd combinations
rather than to generate large volumes of data.

The code is not complex, though fiddling with the dependencies was tedious. The result is a Java program that will 
generate a Parquet file without the need of Hive, HDFS or other Ecos. You can specify the exact Parquet schema you 
want, as well as encoding, page sizes and all the other Parquet “knobs.” Data is generated in the program (which 
is handy for testing the more obscure data types), but you could read it from, say, as CSV file.

This program is based on an example from [this blog post](http://php.sabscape.com/blog/?p=623) on how to
write a file using the Hive serde support.

While this example uses the Hive mechanisms, it does not need to be run under Hive. Instead, it is stand-alone
though it does include Hive and HDFS libraries.

## Parquet Schema

Parquet provides an obscure feature that lets you define file schema using a text expression. Here is an
example:

    message test { required int32 index; required int32 value (DATE); required int32 raw; }

A note in the Parquet code says that the syntax follows that in the 
[Google Dremel paper](http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36632.pdf). The one addition seems to be the syntax for specifying the Parquet logical type (the "(DATE)" bit in the second column
above). See [this page](https://github.com/Parquet/parquet-format/blob/master/LogicalTypes.md) 
for the Parquet logical types.

## File Builder

The heart of this example is the `SimpleParquetWriter` class. It wraps up a bunch of cruft to provide a simple interface
for creating files using Hive-style "writable" classes: `IntWritable`, `LongWritable`, and so on. Parquet provides
the `Builder` class to create a Parquet writer. The code here defines a subclass,
`SimpleBuilder` to create a Hive-style writer.
The `SimpleParquetWriter` class is a thin wrapper on top of the `Builder` to make it easy to programmatically
write files using a default set of Parquet attribute. A constructor is available to let you set
all the Parquet "knobs" programatically using the `SimpleBuilder`, then us this to create your
simple writer. 

## Writing a File

The `BuildFilesDirect` shows an example of creating a file. Start with a schema, then write data to the file
using the `Writable` classes. The examples here write just a few "special" values, such as 0 and extreme
values. The blog post shown above shows how to generate millions of rows of randomized data.S

## CSV-based Extension

Not in this code, but a possible extension, is to get the data and schema from a CSV file. Maybe something like:

    index,value,raw
    required int32,required int32 (Date),required int32
    1, 0, 0
    2, -1, -1
    ...
    
That was not helpful for the particular tests created here (where it was handy to create data in a program),
but is something we could add.