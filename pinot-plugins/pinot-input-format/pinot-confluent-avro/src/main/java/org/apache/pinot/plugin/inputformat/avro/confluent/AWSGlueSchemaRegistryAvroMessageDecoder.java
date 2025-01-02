/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.inputformat.avro.confluent;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.StreamMessageDecoder;


/**
 * Decodes avro messages with confluent schema registry.
 * First byte is MAGIC = 0, second 4 bytes are the schema id, the remainder is the value.
 * NOTE: Do not use schema in the implementation, as schema will be removed from the params
 */
public class AWSGlueSchemaRegistryAvroMessageDecoder implements StreamMessageDecoder<byte[]> {
    private GlueSchemaRegistryKafkaDeserializer _deserializer;
    private RecordExtractor<GenericData.Record> _avroRecordExtractor;
    private String _topicName;

    @Override
    public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
            throws Exception {
        _deserializer = new GlueSchemaRegistryKafkaDeserializer(props);
        Preconditions.checkNotNull(topicName, "Topic must be provided");
        _topicName = topicName;
        _avroRecordExtractor = PluginManager.get().createInstance(AvroRecordExtractor.class.getName());
        _avroRecordExtractor.init(fieldsToRead, null);
    }

    @Override
    public GenericRow decode(byte[] payload, GenericRow destination) {
        GenericData.Record avroRecord = (GenericData.Record) _deserializer.deserialize(_topicName, payload);
        return _avroRecordExtractor.extract(avroRecord, destination);
    }

    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
        return decode(Arrays.copyOfRange(payload, offset, offset + length), destination);
    }
}