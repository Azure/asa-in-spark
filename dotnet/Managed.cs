using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Microsoft.Streaming.QueryRuntime.Abstraction.RuntimeTypesContracts;
using Microsoft.EventProcessing.TranslatorTransform;
using Microsoft.EventProcessing.SteamR.Sql;
using Microsoft.EventProcessing.SqlQueryRunner;
using Microsoft.EventProcessing.Compiler;
using Microsoft.EventProcessing.RuntimeTypes;
using Microsoft.EventProcessing.SteamR.Sql.Runtime;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Subjects;
using System.IO;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Apache.Arrow.Memory;
using Apache.Arrow;

public class ASAHost{
  private readonly Subject<IRecord> input = new Subject<IRecord>();
  private readonly Dictionary<string, IObservable<IRecord>> outputs;

  private Queue<IRecord> outputRecords = new Queue<IRecord>();

  private IRecordSchema schema;

  private Schema outputArrowSchema;

  private NativeMemoryAllocator memoryAllocator;

  private IRecord outputCurrent;

  private static readonly DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

  private const int MillisecToTickRatio = 1000;

  private const int MicrosecToMillisecRatio = 10;

  private UnmanagedMemoryStream outputBuffer;

  private UnmanagedMemoryStream inputBuffer;

  private ArrowStreamReader reader;

  private ArrowStreamWriter writer;
  
  public ASAHost(string sql) {
    //Console.WriteLine("The sql query is: " + sql);
    // Console.WriteLine(inputSchema);

    this.outputs = SqlQueryRunner.Query(
            sql,
            new CompilerConfig() {SqlCompatibility = new SqlCompatibility()},
            ClrFramework.NetStandard20,
            QueryHelper.BinLocations[ClrFramework.NetStandard20],
            new Dictionary<string, Subject<IRecord>>() { { "input", this.input } });

    if (this.outputs.Count != 1)
      throw new ArgumentException("Query: '" + sql + "' returned 0 or more than 1 output: " + this.outputs.Count);

    this.outputs.First().Value.Subscribe(r => this.outputRecords.Enqueue(r));

    this.memoryAllocator = new NativeMemoryAllocator(alignment: 64);
  }

  public void nextOutputRecord_i() {
    this.outputCurrent = this.outputRecords.Dequeue();
  }

  public int pushRecord_i() {
    // reset the position of the buffer and do a full batch read;
    outputBuffer.Position = 0;

    RecordBatch currentBatch = reader.ReadNextRecordBatch();
    
    for (int i = 0; i < currentBatch.Length; i ++)
    {
      var newInputRow = new object[this.schema.Ordinals.Count];

      for (int j = 0; j < this.schema.Ordinals.Count; j ++)
      {
        var array_j = currentBatch.Arrays.ElementAt(j);
        switch (array_j.Data.DataType.TypeId)
        {
          case ArrowTypeId.Int32:
            newInputRow[j] = (Int64)(((Int32Array)array_j).Values[i]);
            break;
          case ArrowTypeId.Int64:
            newInputRow[j] = ((Int64Array)array_j).Values[i];
            break;
          case ArrowTypeId.Float:
            newInputRow[j] = (Double)(((FloatArray)array_j).Values[i]);
            break;
          case ArrowTypeId.Double:
            newInputRow[j] = ((DoubleArray)array_j).Values[i];
            break;
          case ArrowTypeId.String:
            newInputRow[j] = ((StringArray)array_j).GetString(i);
            break;
          case ArrowTypeId.Timestamp:
            TimestampArray tArray = (TimestampArray)array_j;
            var type = (TimestampType) tArray.Data.DataType;
            double timeStampMilli = tArray.Values[i] / MillisecToTickRatio; 
            DateTime dtDateTime = epoch.AddMilliseconds(timeStampMilli);
            newInputRow[j] = dtDateTime;
            break;
          case ArrowTypeId.Binary:
            newInputRow[j] = ((BinaryArray)array_j).GetBytes(i).ToArray();
            break;
          case ArrowTypeId.Boolean:
            newInputRow[j] = ((BooleanArray)array_j).GetBoolean(i);
            break;
          default:
            throw new Exception("Unsupported Arrow array type: " + array_j.Data.DataType.TypeId); 
        }
      }

      this.input.OnNext(Record.Create(this.schema, newInputRow));
    }

    //Write outputs if there is any
    if (this.outputRecords.Count > 0)
    {
      List<IRecord> rows = new List<IRecord>();
      while (this.outputRecords.Count > 0)
      {
        rows.Add(this.outputRecords.Dequeue());
      }

      var recordBatch = createOutputRecordBatch(rows);
      WriteRecordBatch(recordBatch); 
      
      return recordBatch.Length;
    }
    
    return 0;
  }

  private RecordBatch createOutputRecordBatch(List<IRecord> rows)
  {
    var recordBatchBuilder = new RecordBatch.Builder(memoryAllocator);

    for(int i = 0; i < this.outputArrowSchema.Fields.Count; i ++)
    {
      var field = this.outputArrowSchema.GetFieldByIndex(i);
      switch (field.DataType.TypeId)
      {
         case ArrowTypeId.Int64:
           recordBatchBuilder.Append(field.Name, field.IsNullable, col => col.Int64(
                             array => array.AppendRange(rows.Select(row => Convert.ToInt64(row[i])))));
           break;
         case ArrowTypeId.Double:
           recordBatchBuilder.Append(field.Name, field.IsNullable, col => col.Double(
                             array => array.AppendRange(rows.Select(row => Convert.ToDouble(row[i])))));
           break;
         case ArrowTypeId.String:
           recordBatchBuilder.Append(field.Name, field.IsNullable, col => col.String(
                             array => array.AppendRange(rows.Select(row => Convert.ToString(row[i])))));
           break;
         case ArrowTypeId.Timestamp:
           recordBatchBuilder.Append(field.Name, field.IsNullable, col => col.Int64(
                             array => array.AppendRange(rows.Select(row => (((DateTime)row[i]).Ticks - epoch.Ticks) / MicrosecToMillisecRatio))));
           break;
         case ArrowTypeId.Binary:
           recordBatchBuilder.Append(field.Name, field.IsNullable, col => col.Binary(
                             array => array.AppendRange(rows.Select(row => (byte[])(row[i])))));
           break;
         case ArrowTypeId.Boolean:
           recordBatchBuilder.Append(field.Name, field.IsNullable, col => col.Boolean(
                             array => array.AppendRange(rows.Select(row => Convert.ToBoolean(row[i])))));
           break;
         default: throw new Exception("Unsupported Arrow type of output arrow schema: " + field.DataType.TypeId);
      }
    }

    return recordBatchBuilder.Build();
  }

  private void WriteRecordBatch(RecordBatch batch)
  {
      inputBuffer.Position = 0;
      writer.WriteRecordBatchAsync(batch).GetAwaiter().GetResult();
  }

  public int pushComplete_i()
  { 
    this.input.OnCompleted();

    if (this.outputRecords.Count > 0)
    {
      List<IRecord> rows = new List<IRecord>();
      while (this.outputRecords.Count > 0)
      {
        rows.Add(this.outputRecords.Dequeue());
      }

      var recordBatch = createOutputRecordBatch(rows);
      WriteRecordBatch(recordBatch);

      return recordBatch.Length;
    }

    return 0;
  }

  public static IntPtr createASAHost(IntPtr sqlPtr, int sqlLength) {
    string sql = Marshal.PtrToStringUni(sqlPtr, sqlLength);
    GCHandle gch = GCHandle.Alloc(new ASAHost(sql));

    return GCHandle.ToIntPtr(gch);
  }

  public static void registerArrowMemory(IntPtr ptr, IntPtr outputBuffer,
          int outputBufferLength, IntPtr inputBuffer, int inputBufferLength) {
    
    Console.WriteLine("Registering shared memory===============================================================================");   
    ((ASAHost)GCHandle.FromIntPtr(ptr).Target).registerArrowMemoryInner(outputBuffer, outputBufferLength,
        inputBuffer, inputBufferLength);
  }

  public void registerArrowMemoryInner(IntPtr outputBuffer, int outputBufferLength, IntPtr inputBuffer, int inputBufferLength)
  {
    unsafe{
      this.outputBuffer = new UnmanagedMemoryStream((byte*) outputBuffer.ToPointer(), outputBufferLength);
      this.inputBuffer = new UnmanagedMemoryStream((byte*) inputBuffer.ToPointer(), inputBufferLength, inputBufferLength, FileAccess.Write);
    }
  }

  private static IRecordSchema ArrowSchemaToASARecordSchema(Schema arrowSchema)
  {
    var asaFields = arrowSchema.Fields
        .Select(kv => RecordSchema.Property(kv.Key, ArrowTypeToASAType(kv.Value.DataType)))
        .ToArray();
    return RecordSchema.CreateStrict(asaFields);
  }

  private static ITypeSchema ArrowTypeToASAType(IArrowType arrowType)
  {
    switch(arrowType.TypeId)
    {
      case ArrowTypeId.Int32:
      case ArrowTypeId.Int64:
        return PrimitiveSchema.BigintSchema;
      case ArrowTypeId.Float:
      case ArrowTypeId.Double:
        return PrimitiveSchema.DoubleSchema;
      case ArrowTypeId.String:
        return PrimitiveSchema.StringSchema;
      case ArrowTypeId.Timestamp:
        return PrimitiveSchema.DateTimeSchema;
      case ArrowTypeId.Binary:
        return PrimitiveSchema.BinarySchema;      
      case ArrowTypeId.Boolean:
        return PrimitiveSchema.BitSchema;
      default: throw new Exception("Unsupported Arrow type: " + arrowType.TypeId);
    }
  }

  private static IArrowType ASATypeToArrowType(ITypeSchema asaTypeSchema)
  {
    if (asaTypeSchema == PrimitiveSchema.BigintSchema)
      return Int64Type.Default;
    if (asaTypeSchema == PrimitiveSchema.DoubleSchema)
      return DoubleType.Default;
    if (asaTypeSchema == PrimitiveSchema.StringSchema)
      return StringType.Default;
    if (asaTypeSchema == PrimitiveSchema.DateTimeSchema)
      return new TimestampType(TimeUnit.Microsecond, "UTC");
    if (asaTypeSchema == PrimitiveSchema.BinarySchema)
      return BinaryType.Default;
    if (asaTypeSchema == PrimitiveSchema.BitSchema)
      return BooleanType.Default;
    if (asaTypeSchema == PrimitiveSchema.ObjectSchema)
      throw new Exception("Unsupport ASA type Object type.");
    throw new Exception("Unsupport ASA type: " + asaTypeSchema); 
  }

  public static void getOutputSchema(IntPtr ptr, IntPtr sqlPtr, int sqlLen)
  {
    string sql = Marshal.PtrToStringUni(sqlPtr, sqlLen); 
    ((ASAHost)GCHandle.FromIntPtr(ptr).Target).getOutputSchemaInner(sql);
  }

  public void getOutputSchemaInner(string sql) {
    reader = new ArrowStreamReader(this.outputBuffer, leaveOpen: false);
    // reader one batch to get the arrow schema first
    reader.ReadNextRecordBatch();

    this.schema = ArrowSchemaToASARecordSchema(reader.Schema);

    var result =
                SqlCompiler.Compile(
                    sql,
                    new QueryBindings(
                        new Dictionary<string, InputDescription> { { "input", new InputDescription(this.schema, InputType.Stream) } }) );

    var step = result.Steps.First();

    Schema.Builder builder = new Schema.Builder();
    foreach (KeyValuePair<string, int> kv in step.Output.PayloadSchema.Ordinals.OrderBy(kv => kv.Value))
    {
        builder = builder.Field(f => f.Name(kv.Key).DataType(ASATypeToArrowType(step.Output.PayloadSchema[kv.Value].Schema)).Nullable(false));
    }

    this.outputArrowSchema = builder.Build();

    this.writer = new ArrowStreamWriter(this.inputBuffer, this.outputArrowSchema);
    //Write empty batch to send the schema to Java side
    var emptyRecordBatch = createOutputRecordBatch(new List<IRecord>());

    WriteRecordBatch(emptyRecordBatch);
  }

  public static void stringFree(IntPtr strPtr) {
    Marshal.FreeHGlobal(strPtr);
  }

//////////////////////////////////////////////////////////////////////////////

  public static void nextOutputRecord(IntPtr ptr) { 
    ((ASAHost)GCHandle.FromIntPtr(ptr).Target).nextOutputRecord_i();
  }

  public static int pushRecord(IntPtr ptr) {
    return ((ASAHost)GCHandle.FromIntPtr(ptr).Target).pushRecord_i();
  }
  
  public static int pushComplete(IntPtr ptr) {
    return ((ASAHost)GCHandle.FromIntPtr(ptr).Target).pushComplete_i();
  }
  
  public static void deleteASAHost(IntPtr ptr) {
   var gch = GCHandle.FromIntPtr(ptr);
   // ((ASAHost)gch.Target).close();  // TODO
    gch.Free();
  }
}
