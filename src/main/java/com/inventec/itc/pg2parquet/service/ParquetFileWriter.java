package com.inventec.itc.pg2parquet.service;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.avro.Schema.Parser;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class ParquetFileWriter {

    private Schema schema;

    public ParquetFileWriter() {
        // 定义Avro Schema
        String schemaString = "{"
                + "\"type\":\"record\","
                + "\"name\":\"MyRecord\","
                + "\"fields\":["
                + "{\"name\":\"id\",\"type\":\"int\"},"
                + "{\"name\":\"name\",\"type\":\"string\"}"
                + "]"
                + "}";
        schema = new Parser().parse(schemaString);
    }

    public void writeToParquet(Struct after) {
        // 创建ParquetWriter
        Path path = new Path("/path/to/output.parquet");
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(path)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

            // 将CDC数据转换为Avro格式
            GenericRecord record = new GenericData.Record(schema);
            record.put("id", after.get("id"));
            record.put("name", after.get("name"));

            // 写入Parquet文件
            writer.write(record);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
