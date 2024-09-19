package com.inventec.itc.pg2parquet.batch;

import com.inventec.itc.pg2parquet.service.ParquetFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.AbstractBatchConfiguration;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration extends AbstractBatchConfiguration {

    @Bean
    public Step step1(StepBuilderFactory stepBuilderFactory, ItemWriter<GenericRecord> writer) {
        return stepBuilderFactory.get("cdcToParquetStep")
                .<GenericRecord, GenericRecord>chunk(100)
                .reader(cdcItemReader())  // 自定义 ItemReader 从 Debezium 获取数据
                .writer(writer)
                .build();
    }

    @Bean
    public Job job(JobBuilderFactory jobBuilderFactory, Step step1) {
        return jobBuilderFactory.get("cdcToParquetJob")
                .start(step1)
                .build();
    }

    @Bean
    public CdcItemReader cdcItemReader() {
        return new CdcItemReader();  // 实现自定义的 ItemReader
    }

    @Bean
    public ItemWriter<GenericRecord> writer() {
        return new ParquetFileWriter();  // 之前实现的 ParquetFileWriter
    }
}