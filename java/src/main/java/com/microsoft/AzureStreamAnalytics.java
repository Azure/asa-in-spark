package com.microsoft;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.TimeZone;
import java.sql.Timestamp;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.io.*;
import java.lang.Long;
import scala.collection.JavaConverters;
import org.joda.time.DateTimeZone;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.arrow.vector.types.*;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.spark.sql.execution.arrow.ArrowUtils;
import org.apache.arrow.memory.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.arrow.ArrowWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.spark.sql.vectorized.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import scala.collection.JavaConversions;

public class AzureStreamAnalytics
{
    static final Logger logger = Logger.getLogger(AzureStreamAnalytics.class);    

    private static final int INTEROP_BUFFER_SIZE = 1024 * 1024; //1MB

    private static final int ARROW_BATCH_SIZE = 100; //number of rows to transfer from JVM to DOTNET through arrow in each batch

    private static Iterator<Row> processRows(
            String sql,
            StructType schema,
            Iterator<Row> iter) throws IOException
    {
        final long ptr = ASANative.startASA(sql);

        //register byte buffer memory address to ASAnative
        ByteBuffer outputBuffer = ByteBuffer.allocateDirect(INTEROP_BUFFER_SIZE);
        ByteBuffer inputBuffer = ByteBuffer.allocateDirect(INTEROP_BUFFER_SIZE);

        ASANative.registerArrowMemory(outputBuffer, outputBuffer.capacity(),
                        inputBuffer, inputBuffer.capacity(), ptr);

        Schema arrowSchema = ArrowUtils.toArrowSchema(schema, DateTimeZone.UTC.getID());

        //Setup the arrow writer
        RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
        ByteBufferBackedOutputStream outputStream = new ByteBufferBackedOutputStream(outputBuffer);

        BufferAllocator writeAllocator = rootAllocator.newChildAllocator("Arrow writer to C# world", 0, Long.MAX_VALUE);
        VectorSchemaRoot outputRoot = VectorSchemaRoot.create(arrowSchema, writeAllocator);
        ArrowWriter arrowWriter = ArrowWriter.create(outputRoot);
        ArrowStreamWriter writer = new ArrowStreamWriter(outputRoot, null, outputStream);

        BufferAllocator readAllocator = rootAllocator.newChildAllocator("Arrow reader from C# world", 0, Long.MAX_VALUE);
        ArrowStreamReader reader = new ArrowStreamReader(new ByteBufferBackedInputStream(inputBuffer), readAllocator);
        VectorSchemaRoot readRoot = null;

        try
        {
            //Wrtie the input schema to shared memory
            WriteChannel writeChannel = new WriteChannel(Channels.newChannel(outputStream));
            MessageSerializer.serialize(writeChannel, arrowSchema);
            // need to start ASA on driver to get the output schema
            // The output schema is also encoded in the shared errow memory as message header
            // we need to read it from the shared memory
            ASANative.getOutputSchema(sql, ptr);

            readRoot = reader.getVectorSchemaRoot();
            writer.start();
        }
        catch (IOException e)
        {
            logger.debug(e.getMessage());
            throw e;
        }

        ExpressionEncoder<Row> inputEncoder = RowEncoder.apply(schema); 
        //Get Schema from arrow batch header and setup spark encoder based on the schema
        StructType encodeSchema = ArrowUtils.fromArrowSchema(readRoot.getSchema());
        logger.info("Encode Schema:        " + encodeSchema.toString());

        List<Attribute> attributes = JavaConversions.asJavaCollection(encodeSchema.toAttributes()).stream()
            .map(Attribute::toAttribute).collect(Collectors.toList());
        ExpressionEncoder<Row> encoder = RowEncoder.apply(encodeSchema).resolveAndBind(
            JavaConverters.asScalaIteratorConverter(attributes.iterator()).asScala().toSeq(),
            org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$.MODULE$);
    
        return new Iterator<Row>() {
            private int rowsToFetch = 0;
            private Iterator<InternalRow> internalRowIterator = null;
            private boolean completePushed = false;

            public boolean hasNext() { 
                // have more rows that need fetching from native side
                if (this.rowsToFetch > 0)
                    return true;

                // are there anymore input rows?
                if (!iter.hasNext()) {
                    if (this.completePushed) {
                        ASANative.stopAndClean(ptr);
            //stop the reader as well
                        return false;
                    }

                    // signal data end
                    this.rowsToFetch = ASANative.pushComplete(ptr);
                    this.completePushed = true;
                }
                else {
                    do {
                        int currentBatchRowCount = 0;
                        
                        do {
                            // get next row
                            Row r = iter.next();

                            //arrowWriter.write(InternalRow.fromSeq(r.toSeq()));
                            arrowWriter.write(inputEncoder.toRow(r));
                            currentBatchRowCount ++;

                            // TODO: round-robin for multiple inputs
                        } while (iter.hasNext() && currentBatchRowCount < ARROW_BATCH_SIZE);

                        try
                        {
                            //Write Batch
                            outputBuffer.rewind();
                            arrowWriter.finish();
                            writer.writeBatch();
                            this.rowsToFetch = ASANative.pushRecord(ptr);
                            //C# side finished reading the buffer so let's rewind the shared buffer
                            arrowWriter.reset();

                            if (this.rowsToFetch == 0 && !iter.hasNext()) {
                                this.rowsToFetch = ASANative.pushComplete(ptr);
                                this.completePushed = true;
                                writer.end();
                                outputRoot.close();
                                writeAllocator.close();
                            }
                        }
                        catch (IOException e)
                        {
                            logger.debug(e.getMessage());
                            throw new RuntimeException(e);
                        }
                    } while (this.rowsToFetch == 0 && iter.hasNext());
                }
                
                //We reach here if rowsToFetch > 0 or !iter.hasNext()
                return this.rowsToFetch > 0;
            }

            public Row next() { 
                this.rowsToFetch--;

                if (internalRowIterator == null || !internalRowIterator.hasNext())
                {
                    //Ask native to load next batch in buffer
                    inputBuffer.rewind();
                    try
                    {
                        VectorSchemaRoot readRoot = reader.getVectorSchemaRoot();
                        //load the next batch
                        boolean loadedBatch = reader.loadNextBatch();
                        if (loadedBatch && readRoot.getRowCount() > 0)
                        {
                            ColumnVector[] list = new ColumnVector[readRoot.getFieldVectors().size()];

                            for (int i = 0; i < readRoot.getFieldVectors().size(); i ++)
                            {
                                list[i] = new ArrowColVector(readRoot.getFieldVectors().get(i));
                            }
                              
                            ColumnarBatch columnarBatch = new ColumnarBatch(list);  
                            columnarBatch.setNumRows(readRoot.getRowCount());
                            internalRowIterator = columnarBatch.rowIterator();
                        }
                        else
                        {
                            throw new UnsupportedOperationException("Could load batch but there is more rows to Fetch!");
                        }
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                }
    
                return encoder.fromRow(internalRowIterator.next());
            }
        };
    }

    //This is the main function of executing ASA query
    //It parses the schema and calls mapPartitions to process the each partition 
    public static Dataset<Row> execute(String query, Dataset<Row> input)
    {
        logger.setLevel(Level.INFO);
        StructType sparkSchema = input.schema();

        final long ptr = ASANative.startASA(query);

        //register byte buffer memory address to ASAnative
        ByteBuffer outputBuffer = ByteBuffer.allocateDirect(INTEROP_BUFFER_SIZE);
        ByteBuffer inputBuffer = ByteBuffer.allocateDirect(INTEROP_BUFFER_SIZE);

        ASANative.registerArrowMemory(outputBuffer, outputBuffer.capacity(),
                        inputBuffer, inputBuffer.capacity(), ptr);

        Schema arrowSchema = ArrowUtils.toArrowSchema(sparkSchema, DateTimeZone.UTC.getID());
    
        //Setup the arrow writer
        RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
        ByteBufferBackedOutputStream outputStream = new ByteBufferBackedOutputStream(outputBuffer);

        BufferAllocator writeAllocator = rootAllocator.newChildAllocator("Arrow writer to C# world", 0, Long.MAX_VALUE);
        VectorSchemaRoot outputRoot = VectorSchemaRoot.create(arrowSchema, writeAllocator);
        ArrowWriter arrowWriter = ArrowWriter.create(outputRoot);
        ArrowStreamWriter writer = new ArrowStreamWriter(outputRoot, null, outputStream);

        BufferAllocator readAllocator = rootAllocator.newChildAllocator("Arrow reader from C# world", 0, Long.MAX_VALUE);
        ArrowStreamReader reader = new ArrowStreamReader(new ByteBufferBackedInputStream(inputBuffer), readAllocator);
        VectorSchemaRoot readRoot = null;

        try
        {
            //Wrtie the input schema to shared memory
            WriteChannel writeChannel = new WriteChannel(Channels.newChannel(outputStream));
            MessageSerializer.serialize(writeChannel, arrowSchema);
          

            // need to start ASA on driver to get the output schema
            // The output schema is also encoded in the shared errow memory as message header
            // we need to read it from the shared memory
            ASANative.getOutputSchema(query, ptr);

            readRoot = reader.getVectorSchemaRoot();
        }
        catch (IOException e)
        {
            logger.debug(e.getMessage());
            throw new RuntimeException(e);
        }

        logger.info("Read Schema:        " + readRoot.getSchema().toString());   
        StructType outputSchema = ArrowUtils.fromArrowSchema(readRoot.getSchema());
        logger.info("ASA InputSchema: " + sparkSchema + " OutputSchema: "+ outputSchema);

        Encoder<Row> encoder = RowEncoder.apply(outputSchema);
        
        //Use Apache Arrow
        return input.mapPartitions(
            iter -> processRows(query,
                sparkSchema,
                iter),
            encoder);
    }

    //helper class to wrap output/input stream around the buffer
    public static class ByteBufferBackedOutputStream extends OutputStream{
        public ByteBuffer buf;
        ByteBufferBackedOutputStream( ByteBuffer buf){
            this.buf = buf;
        }
        
        public synchronized void write(int b) throws IOException {
            buf.put((byte) b);
        }

        public synchronized void write(byte[] bytes, int off, int len) throws IOException {
            buf.put(bytes, off, len);
        }
    }

    public static class ByteBufferBackedInputStream extends InputStream{
        public ByteBuffer buf;
        ByteBufferBackedInputStream( ByteBuffer buf){
            this.buf = buf;
        }
      
        public synchronized int read() throws IOException {
            if (!buf.hasRemaining()) {
                return -1;
            }
            return buf.get();
        }
        
        public synchronized int read(byte[] bytes, int off, int len) throws IOException {
            len = Math.min(len, buf.remaining());
            buf.get(bytes, off, len);
            return len;
        }  
    }
}
