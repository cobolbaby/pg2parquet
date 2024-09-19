package com.inventec.itc.pg2parquet.batch;

import org.apache.avro.generic.GenericRecord;
import org.springframework.batch.item.ItemReader;
import org.springframework.stereotype.Component;

import java.util.Queue;
import java.util.LinkedList;

@Component
public class CdcItemReader implements ItemReader<GenericRecord> {

    private final Queue<GenericRecord> recordQueue = new LinkedList<>();

    // 这个方法将从队列中读取记录
    @Override
    public GenericRecord read() {
        return recordQueue.poll();  // 从队列中取出下一个记录
    }

    // 用于将 CDC 记录放入队列（假设在某处调用此方法以填充队列）
    public void addRecord(GenericRecord record) {
        recordQueue.add(record);
    }
}
