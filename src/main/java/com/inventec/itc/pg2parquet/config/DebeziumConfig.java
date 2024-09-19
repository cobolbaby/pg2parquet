package com.inventec.itc.pg2parquet.config;

import com.inventec.itc.pg2parquet.batch.CdcItemReader;
import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.RecordChangeEvent;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@org.springframework.context.annotation.Configuration
public class DebeziumConfig {

    @Autowired
    private CdcItemReader cdcItemReader;

    @Bean
    public EmbeddedEngine debeziumEngine() {
        io.debezium.config.Configuration config = io.debezium.config.Configuration.create()
                .with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                .with("database.hostname", "localhost")
                .with("database.port", "5432")
                .with("database.user", "postgres")
                .with("database.password", "password")
                .with("database.dbname", "mydb")
                .with("database.server.name", "pgserver")
                .with("table.include.list", "public.my_table")
                .with("plugin.name", "pgoutput")
                .with("slot.name", "debezium_slot")
                .with("publication.name", "debezium_pub")
                .build();

        return EmbeddedEngine.create()
                .using(config)
                .notifying(this::handleEvent)
                .build();
    }

    private void handleEvent(SourceRecord record) {
        Struct sourceRecordValue = (Struct) record.value();
        Struct after = sourceRecordValue.getStruct("after");

        // 将CDC数据转换为GenericRecord（或其他适当格式）
        GenericRecord genericRecord = convertToGenericRecord(after);

        // 将记录添加到 CdcItemReader 的队列中
        cdcItemReader.addRecord(genericRecord);
    }

    private GenericRecord convertToGenericRecord(Struct after) {
        // TODO: 实现数据转换逻辑
        return null;
    }
}