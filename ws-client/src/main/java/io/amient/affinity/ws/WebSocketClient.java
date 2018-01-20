/*
 * Copyright 2016 Michal Harish, michal.harish@gmail.com
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.amient.affinity.ws;

import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.avro.util.ByteBufferOutputStream;

import javax.websocket.*;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;

@ClientEndpoint
final public class WebSocketClient {

    public interface TextMessageHandler {
        void onMessage(String message);
        void onError(Throwable e);
    }

    public interface AvroMessageHandler {
        void onMessage(Object message);
        void onError(Throwable e);
    }

    //TODO #28 enforce the entire class is Trhead-Safe (websocket can only communicate one message in each direction)
    private Session session;
    private Map<Integer, Schema> schemas = new HashMap<>();
    private Map<String, Integer> types = new HashMap<>();
    private Queue<Map.Entry<Integer, byte[]>> queue = new LinkedList<>();
    final private TextMessageHandler textMessageHandler;
    final private AvroMessageHandler avroMessageHandler;

    public WebSocketClient(URI endpointURI, AvroMessageHandler avroMessageHandler) {
        try {
            //TODO #28 reuse container for multiple websockets to different entities
            WebSocketContainer container = ContainerProvider.getWebSocketContainer();
            container.connectToServer(this, endpointURI);
            this.avroMessageHandler = avroMessageHandler;
            this.textMessageHandler = null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public WebSocketClient(URI endpointURI, TextMessageHandler textMessageHandler) {
        try {
            //TODO #28 reuse container for multiple websockets to different entities
            WebSocketContainer container = ContainerProvider.getWebSocketContainer();
            container.connectToServer(this, endpointURI);
            this.textMessageHandler = textMessageHandler;
            this.avroMessageHandler = null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() throws IOException {
        if (session != null) session.close();
    }

    @OnOpen
    public void onOpen(Session session) {
        this.session = session;
    }

    @OnError
    public void onError(Session session, Throwable e) {
        if (textMessageHandler != null) textMessageHandler.onError(e);
        if (avroMessageHandler != null) avroMessageHandler.onError(e);
    }

    @OnClose
    public void onClose(Session userSession, CloseReason reason) {
        this.session = null;
    }

    @OnMessage
    public void onTextMessage(String message) {
        if (textMessageHandler != null) textMessageHandler.onMessage(message);
    }

    @OnMessage
    public void onMessage(byte[] message) throws IOException {
        if (message.length == 0) {
            if (avroMessageHandler != null) {
                //FIXME #28 what should really be sent is a typed empty record which comes back to the API design issue of representing a zero-value of an avro record
                avroMessageHandler.onMessage(null);
            }
            return;
        }
        ByteBuffer buf = ByteBuffer.wrap(message);
        int magic = buf.get();
        int schemaId = buf.getInt();
        switch (magic) {
            case 0:
                byte[] bytes = new byte[buf.remaining()];
                buf.get(bytes);
                if (!schemas.containsKey(schemaId)) {
                    queue.add(new AbstractMap.SimpleEntry<Integer, byte[]>(schemaId, bytes));
                    ByteBufferOutputStream req = new ByteBufferOutputStream();
                    DataOutputStream reqData = new DataOutputStream(req);
                    reqData.write(123);
                    reqData.writeInt(schemaId);
                    for (ByteBuffer byteBuffer : req.getBufferList()) {
                        session.getAsyncRemote().sendBinary(byteBuffer);
                    }
                } else {
                    Schema schema = schemas.get(schemaId);
                    processIncomingBuffer(bytes, schema);
                }
                break;
            case 123:
                Schema schema = new Schema.Parser().parse(new ByteBufferInputStream(Arrays.asList(buf)));
                synchronized (this) {
                    schemas.put(schemaId, schema);
                    types.put(schema.getFullName(), schemaId);
                    notifyAll();
                }
                if (queue.size() > 0) {
                    Iterator<Map.Entry<Integer, byte[]>> it = queue.iterator();
                    while (it.hasNext()) {
                        Map.Entry<Integer, byte[]> next = it.next();
                        if (next.getKey() == schemaId) {
                            processIncomingBuffer(next.getValue(), schema);
                        }
                    }
                }
                break;
        }

    }

    private void processIncomingBuffer(byte[] buf, Schema schema) throws IOException {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(buf, null);
        GenericDatumReader<Object> reader = new GenericDatumReader<>(schema, schema);
        Object record = reader.read(null, decoder);
        avroMessageHandler.onMessage(record);
    }


    final public void send(String message) {
        this.session.getAsyncRemote().sendText(message);
    }

    final public void send(IndexedRecord record) throws IOException {
        try {
            Schema schema = record.getSchema();
            if (!types.containsKey(schema.getFullName())) {
                throw new IllegalArgumentException("Avro Schema not recoginzed for type: " + schema.getFullName());
            }
            int schemaId = types.get(schema.getFullName());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutput dataOut = new DataOutputStream(out);
            dataOut.write(0); //MAGIC_BYTE
            dataOut.writeInt(schemaId); //SCHEMA_ID
            Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
            writer.write(record, encoder);
            encoder.flush();
            out.close();
            this.session.getAsyncRemote().sendBinary(ByteBuffer.wrap(out.toByteArray()));
        } catch (IOException e) {
            throw e;
        }
    }

    public Schema getSchema(String avroType) throws InterruptedException, IOException {
        if (!types.containsKey(avroType)) {
            this.session.getAsyncRemote().sendText(avroType);
            synchronized (this) {
                int i = 5;
                while (session.isOpen() && !types.containsKey(avroType)) {
                    if (--i == 0) throw new IOException("Failed to fetch schema - request time out");
                    wait(1000);
                    if (session == null)  throw new IOException("Failed to fetch schema - socket closed");
                }
            }
        }
        return schemas.get(types.get(avroType));
    }

}
