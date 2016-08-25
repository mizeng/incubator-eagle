/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.evaluator;

import backtype.storm.metric.api.MultiCountMetric;
import org.apache.eagle.alert.engine.Collector;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.evaluator.impl.PolicyGroupEvaluatorImpl;
import org.apache.eagle.alert.engine.evaluator.impl.SiddhiPolicyHandler;
import org.apache.eagle.alert.engine.mock.MockSampleMetadataFactory;
import org.apache.eagle.alert.engine.mock.MockStreamCollector;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class SiddhiCEPPolicyEventHandlerTest {
    private final static Logger LOG = LoggerFactory.getLogger(SiddhiCEPPolicyEventHandlerTest.class);

    private Map<String, StreamDefinition> createDefinition(String ... streamIds) {
        Map<String, StreamDefinition> sds = new HashMap<>();
        for(String streamId:streamIds) {
            // construct StreamDefinition
            StreamDefinition sd = MockSampleMetadataFactory.createSampleStreamDefinition(streamId);
            sds.put(streamId, sd);
        }
        return sds;
    }

    @SuppressWarnings("serial")
    @Test
    public void testBySendSimpleEvent() throws Exception {
        SiddhiPolicyHandler handler;
        MockStreamCollector collector;

        handler = new SiddhiPolicyHandler(createDefinition("sampleStream_1","sampleStream_2"), 0);
        collector = new MockStreamCollector();
        PolicyDefinition policyDefinition = MockSampleMetadataFactory.createSingleMetricSamplePolicy();
        PolicyHandlerContext context = new PolicyHandlerContext();
        context.setPolicyDefinition(policyDefinition);
        context.setPolicyCounter(new MultiCountMetric());
        context.setPolicyEvaluator(new PolicyGroupEvaluatorImpl("evaluatorId"));
        handler.prepare(collector,context);
        StreamEvent event = StreamEvent.Builder()
                .schema(MockSampleMetadataFactory.createSampleStreamDefinition("sampleStream_1"))
                .streamId("sampleStream_1")
                .timestamep(System.currentTimeMillis())
                .attributes(new HashMap<String,Object>(){{
                    put("name","cpu");
                    put("value",60.0);
                    put("bad","bad column value");
                }}).build();
        handler.send(event);
        handler.close();
    }

    @SuppressWarnings("serial")
    @Test
    public void testWithTwoStreamJoinPolicy() throws Exception {
        Map<String,StreamDefinition> ssd = createDefinition("sampleStream_1","sampleStream_2");

        PolicyDefinition policyDefinition = new PolicyDefinition();
        policyDefinition.setName("SampleJoinPolicyForTest");
        policyDefinition.setInputStreams(Arrays.asList("sampleStream_1","sampleStream_2"));
        policyDefinition.setOutputStreams(Collections.singletonList("joinedStream"));
        policyDefinition.setDefinition(new PolicyDefinition.Definition(PolicyStreamHandlers.SIDDHI_ENGINE,
                "from sampleStream_1#window.length(10) as left " +
                "join sampleStream_2#window.length(10) as right " +
                "on left.name == right.name and left.value == right.value " +
                "select left.timestamp,left.name,left.value "+
                "insert into joinedStream"));
        policyDefinition.setPartitionSpec(Collections.singletonList(MockSampleMetadataFactory.createSampleStreamGroupbyPartition("sampleStream_1", Collections.singletonList("name"))));
        SiddhiPolicyHandler handler;
        Semaphore mutex = new Semaphore(0);
        List<AlertStreamEvent> alerts = new ArrayList<>(0);
        Collector<AlertStreamEvent> collector = (event) -> {
            LOG.info("Collected {}",event);
            Assert.assertTrue(event != null);
            alerts.add(event);
            mutex.release();
        };

        handler = new SiddhiPolicyHandler(ssd, 0);
        PolicyHandlerContext context = new PolicyHandlerContext();
        context.setPolicyDefinition(policyDefinition);
        context.setPolicyCounter(new MultiCountMetric());
        context.setPolicyEvaluator(new PolicyGroupEvaluatorImpl("evaluatorId"));
        handler.prepare(collector,context);


        long ts_1 = System.currentTimeMillis();
        long ts_2 = System.currentTimeMillis()+1;

        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream_1"))
                .streamId("sampleStream_1")
                .timestamep(ts_1)
                .attributes(new HashMap<String,Object>(){{
                    put("name","cpu");
                    put("value",60.0);
                    put("bad","bad column value");
                }}).build());

        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream_2"))
                .streamId("sampleStream_2")
                .timestamep(ts_2)
                .attributes(new HashMap<String,Object>(){{
                    put("name","cpu");
                    put("value",61.0);
                }}).build());

        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream_2"))
                .streamId("sampleStream_2")
                .timestamep(ts_2)
                .attributes(new HashMap<String,Object>(){{
                    put("name","disk");
                    put("value",60.0);
                }}).build());

        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream_2"))
                .streamId("sampleStream_2")
                .timestamep(ts_2)
                .attributes(new HashMap<String,Object>(){{
                    put("name","cpu");
                    put("value",60.0);
                }}).build());

        handler.close();

        Assert.assertTrue("Should get result in 5 s",mutex.tryAcquire(5, TimeUnit.SECONDS));
        Assert.assertEquals(1,alerts.size());
        Assert.assertEquals("joinedStream",alerts.get(0).getStreamId());
        Assert.assertEquals("cpu",alerts.get(0).getData()[1]);
    }

    @Test
    public void testWithBatchWindow() throws Exception {
        Map<String,StreamDefinition> ssd = createDefinition("sampleStream_1");

        PolicyDefinition policyDefinition = new PolicyDefinition();
        policyDefinition.setName("SampleJoinPolicyForTest");
        policyDefinition.setInputStreams(Arrays.asList("sampleStream_1"));
        policyDefinition.setOutputStreams(Collections.singletonList("outputStream"));
        policyDefinition.setDefinition(new PolicyDefinition.Definition(PolicyStreamHandlers.SIDDHI_ENGINE,
                "from sampleStream_1[name == \"cpu\"]#window.lengthBatch(2) select name,value,count(value) as cnt,max(value) as max,min(value) as min insert current events into outputStream "));
        policyDefinition.setPartitionSpec(Collections.singletonList(MockSampleMetadataFactory.createSampleStreamGroupbyPartition("sampleStream_1", Collections.singletonList("name"))));
        SiddhiPolicyHandler handler;
        Semaphore mutex = new Semaphore(0);
        List<AlertStreamEvent> alerts = new ArrayList<>(0);
        Collector<AlertStreamEvent> collector = (event) -> {
            LOG.info("Collected {}",event);
            Assert.assertTrue(event != null);
            alerts.add(event);
            mutex.release();
        };

        handler = new SiddhiPolicyHandler(ssd, 0);
        PolicyHandlerContext context = new PolicyHandlerContext();
        context.setPolicyDefinition(policyDefinition);
        context.setPolicyCounter(new MultiCountMetric());
        context.setPolicyEvaluator(new PolicyGroupEvaluatorImpl("evaluatorId"));
        handler.prepare(collector, context);

        long ts_1 = System.currentTimeMillis();


        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream_1"))
                .streamId("sampleStream_1")
                .timestamep(ts_1)
                .attributes(new HashMap<String,Object>(){{
                    put("name","cpu");
                    put("value",60.0);
                    put("bad","bad column value");
                }}).build());

        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream_1"))
                .streamId("sampleStream_1")
                .timestamep(ts_1)
                .attributes(new HashMap<String, Object>() {{
                    put("name", "cpu");
                    put("value", 61.0);
                }}).build());

        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream_1"))
                .streamId("sampleStream_1")
                .timestamep(ts_1)
                .attributes(new HashMap<String, Object>() {{
                    put("name", "disk");
                    put("value", 60.0);
                }}).build());

        for (int i=0; i<3; i++){
            final double value = 62.0+i;
            handler.send(StreamEvent.Builder()
                    .schema(ssd.get("sampleStream_1"))
                    .streamId("sampleStream_1")
                    .timestamep(ts_1)
                    .attributes(new HashMap<String, Object>() {{
                        put("name", "cpu");
                        put("value", value);
                    }}).build());
        }

        handler.close();

        Assert.assertTrue("Should get result in 5 s", mutex.tryAcquire(5, TimeUnit.SECONDS));
        Assert.assertEquals("outputStream", alerts.get(0).getStreamId());
        Assert.assertEquals("cpu", alerts.get(0).getData()[0]);
        Assert.assertEquals(61.0, alerts.get(0).getData()[1]);
        Assert.assertEquals(2l, alerts.get(0).getData()[2]);
        Assert.assertEquals(61.0, alerts.get(0).getData()[3]);
        Assert.assertEquals(60.0, alerts.get(0).getData()[4]);
        Assert.assertEquals(2, alerts.size());
    }

    /***
     * Sample Stream is like "{cpu, 60}, {disk, 1000}, {cpu, 61}, {disk, 1100}, {cpu, 62}, {disk, 1200}, {cpu, 63},
     * {disk, 1300}, {cpu, 64}, {disk, 1400}, {cpu, 61}, {cpu, 61}, {cpu, 62}, {disk, 1100},{cpu, 63}, {disk, 1200},
     * {cpu, 64}"
     * Event number is 17, while Batch window length is 16.
     *
     * Aggregate Stream is like "{cpu, 60, 1}, {cpu, 61, 3}, {cpu, 62, 2}, {cpu, 63, 2}, {cpu, 64, 1},
     * {disk, 1000, 1}, {disk, 1100, 2}, {disk, 1200, 2}, {disk, 1300, 1}, {disk, 1400, 1}"
     * @throws Exception
     */
    @Test
    public void testWithCountInBatchWindow() throws Exception {
        Map<String,StreamDefinition> ssd = createDefinition("sampleStream");

        PolicyDefinition policyDefinition = new PolicyDefinition();
        policyDefinition.setName("SampleAggregatePolicyForTest");
        policyDefinition.setInputStreams(Arrays.asList("sampleStream"));
        policyDefinition.setOutputStreams(Collections.singletonList("aggregateStream"));
        policyDefinition.setDefinition(new PolicyDefinition.Definition(PolicyStreamHandlers.SIDDHI_ENGINE,
                "from sampleStream#window.lengthBatch(16) " +
                        "select name,value,count(value) as cnt " +
                        "group by name,value " +
                        "insert current events into aggregateStream "));
        policyDefinition.setPartitionSpec(Collections.singletonList(MockSampleMetadataFactory.createSampleStreamGroupbyPartition("sampleStream", Collections.singletonList("name"))));
        SiddhiPolicyHandler handler;
        Semaphore mutex = new Semaphore(0);
        List<AlertStreamEvent> alerts = new ArrayList<>(0);
        Collector<AlertStreamEvent> collector = (event) -> {
            LOG.info("Collected {}",event);
            Assert.assertTrue(event != null);
            alerts.add(event);
            mutex.release();
        };

        handler = new SiddhiPolicyHandler(ssd, 0);
        PolicyHandlerContext context = new PolicyHandlerContext();
        context.setPolicyDefinition(policyDefinition);
        context.setPolicyCounter(new MultiCountMetric());
        context.setPolicyEvaluator(new PolicyGroupEvaluatorImpl("evaluatorId"));
        handler.prepare(collector, context);

        long ts = System.currentTimeMillis();

        for (int i=0; i<5; i++){
            final double cpu_value = 60.0+i;
            final double disk_value = 1000.0+100*i;
            handler.send(StreamEvent.Builder()
                    .schema(ssd.get("sampleStream"))
                    .streamId("sampleStream")
                    .timestamep(ts)
                    .attributes(new HashMap<String, Object>() {{
                        put("name", "cpu");
                        put("value", cpu_value);
                    }}).build());
            handler.send(StreamEvent.Builder()
                    .schema(ssd.get("sampleStream"))
                    .streamId("sampleStream")
                    .timestamep(ts)
                    .attributes(new HashMap<String, Object>() {{
                        put("name", "disk");
                        put("value", disk_value);
                    }}).build());
        }
        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream"))
                .streamId("sampleStream")
                .timestamep(ts)
                .attributes(new HashMap<String, Object>() {{
                    put("name", "cpu");
                    put("value", 61.0);
                }}).build());
        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream"))
                .streamId("sampleStream")
                .timestamep(ts)
                .attributes(new HashMap<String, Object>() {{
                    put("name", "cpu");
                    put("value", 61.0);
                }}).build());
        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream"))
                .streamId("sampleStream")
                .timestamep(ts)
                .attributes(new HashMap<String, Object>() {{
                    put("name", "cpu");
                    put("value", 62.0);
                }}).build());
        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream"))
                .streamId("sampleStream")
                .timestamep(ts)
                .attributes(new HashMap<String, Object>() {{
                    put("name", "disk");
                    put("value", 1100.0);
                }}).build());
        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream"))
                .streamId("sampleStream")
                .timestamep(ts)
                .attributes(new HashMap<String, Object>() {{
                    put("name", "cpu");
                    put("value", 63.0);
                }}).build());
        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream"))
                .streamId("sampleStream")
                .timestamep(ts)
                .attributes(new HashMap<String, Object>() {{
                    put("name", "disk");
                    put("value", 1200.0);
                }}).build());
        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream"))
                .streamId("sampleStream")
                .timestamep(ts)
                .attributes(new HashMap<String, Object>() {{
                    put("name", "cpu");
                    put("value", 64.0);
                }}).build());

        handler.close();

        for (AlertStreamEvent ae: alerts){
            if (ae.getData()[0].equals("cpu") && ae.getData()[1].equals(61.0)){
                Assert.assertEquals(ae.getData()[2], 3l);
            }
            if (ae.getData()[0].equals("cpu") && ae.getData()[1].equals(62.0)){
                Assert.assertEquals(ae.getData()[2], 2l);
            }
            if (ae.getData()[0].equals("disk") && ae.getData()[1].equals(1100.0)){
                Assert.assertEquals(ae.getData()[2], 2l);
            }
        }
        Assert.assertEquals(10, alerts.size());
    }

    /***
     * Stream1 is like "{port1, down, label: ed1}, {port2, down, label: ed1}, {port1, down, label: ed1},
     * {port3, down, label: ed2}, {port4, down, label: ed3}, {port5, down, label: ed3}"
     *
     * Stream2 is like "{switch1, down, label: ed1}, {switch2, down, label: ed2}"
     *
     * Joined Stream is like "{port1, down, label: ed1, parent-key: switch1}, {port2, down, label: ed1, parent-key: switch1},
     * {port1, down, label: ed1, parent-key: switch1}, {port3, down, label: ed2, parent-key: switch2},
     * {port4, down, label: ed3, parent-key: }
     *
     * @throws Exception
     */
    @Test
    public void testWithJoinInBatchWindow() throws Exception {
        Map<String,StreamDefinition> ssd = createDefinition("sampleStream_1", "sampleStream_2");

        PolicyDefinition policyDefinition = new PolicyDefinition();
        policyDefinition.setName("SampleJoinPolicyForTest");
        policyDefinition.setInputStreams(Arrays.asList("sampleStream_1", "sampleStream_2"));
        policyDefinition.setOutputStreams(Collections.singletonList("joinStream"));
        policyDefinition.setDefinition(new PolicyDefinition.Definition(PolicyStreamHandlers.SIDDHI_ENGINE,
                "from sampleStream_1#window.timeBatch(5 sec) as left " +
                        "join sampleStream_2#window.timeBatch(5 sec) as right " +
                        "on left.host == right.host " +
                        "select left.name,left.value,left.host,right.name as parent_key " +
                        "group by left.name " +
                        "insert current events into joinStream "));
        policyDefinition.setPartitionSpec(Collections.singletonList(MockSampleMetadataFactory.createSampleStreamGroupbyPartition("sampleStream", Collections.singletonList("name"))));
        SiddhiPolicyHandler handler;
        Semaphore mutex = new Semaphore(0);
        List<AlertStreamEvent> alerts = new ArrayList<>(0);
        Collector<AlertStreamEvent> collector = (event) -> {
            LOG.info("Collected {}",event);
            Assert.assertTrue(event != null);
            alerts.add(event);
            mutex.release();
        };

        handler = new SiddhiPolicyHandler(ssd, 0);
        PolicyHandlerContext context = new PolicyHandlerContext();
        context.setPolicyDefinition(policyDefinition);
        context.setPolicyCounter(new MultiCountMetric());
        context.setPolicyEvaluator(new PolicyGroupEvaluatorImpl("evaluatorId"));
        handler.prepare(collector, context);

        long ts = System.currentTimeMillis();

        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream_1"))
                .streamId("sampleStream_1")
                .timestamep(ts)
                .attributes(new HashMap<String, Object>() {{
                    put("name", "port1");
                    put("value", 1.0);
                    put("host", "ed1");
                }}).build());
        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream_1"))
                .streamId("sampleStream_1")
                .timestamep(ts)
                .attributes(new HashMap<String, Object>() {{
                    put("name", "port2");
                    put("value", 1.0);
                    put("host", "ed1");
                }}).build());
        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream_1"))
                .streamId("sampleStream_1")
                .timestamep(ts)
                .attributes(new HashMap<String, Object>() {{
                    put("name", "port1");
                    put("value", 1.0);
                    put("host", "ed1");
                }}).build());
        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream_1"))
                .streamId("sampleStream_1")
                .timestamep(ts)
                .attributes(new HashMap<String, Object>() {{
                    put("name", "port3");
                    put("value", 1.0);
                    put("host", "ed2");
                }}).build());
        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream_1"))
                .streamId("sampleStream_1")
                .timestamep(ts)
                .attributes(new HashMap<String, Object>() {{
                    put("name", "port4");
                    put("value", 1.0);
                    put("host", "ed3");
                }}).build());
        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream_2"))
                .streamId("sampleStream_2")
                .timestamep(ts)
                .attributes(new HashMap<String, Object>() {{
                    put("name", "switch1");
                    put("value", 1.0);
                    put("host", "ed1");
                }}).build());
        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream_2"))
                .streamId("sampleStream_2")
                .timestamep(ts)
                .attributes(new HashMap<String, Object>() {{
                    put("name", "switch2");
                    put("value", 1.0);
                    put("host", "ed2");
                }}).build());
        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream_1"))
                .streamId("sampleStream_1")
                .timestamep(ts)
                .attributes(new HashMap<String, Object>() {{
                    put("name", "port5");
                    put("value", 1.0);
                    put("host", "ed3");
                }}).build());
        Thread.sleep(5000);
        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream_1"))
                .streamId("sampleStream_1")
                .timestamep(ts)
                .attributes(new HashMap<String, Object>() {{
                    put("name", "port5");
                    put("value", 1.0);
                    put("host", "ed3");
                }}).build());
        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream_1"))
                .streamId("sampleStream_1")
                .timestamep(ts)
                .attributes(new HashMap<String, Object>() {{
                    put("name", "port5");
                    put("value", 1.0);
                    put("host", "ed3");
                }}).build());
        handler.send(StreamEvent.Builder()
                .schema(ssd.get("sampleStream_1"))
                .streamId("sampleStream_1")
                .timestamep(ts)
                .attributes(new HashMap<String, Object>() {{
                    put("name", "port5");
                    put("value", 1.0);
                    put("host", "ed3");
                }}).build());
        Thread.sleep(3000);
        handler.close();

        Thread.sleep(1000);
        for (AlertStreamEvent ae: alerts){
            if (ae.getData()[0].equals("port1")){
                Assert.assertEquals(ae.getData()[3], "switch1");
            }
            if (ae.getData()[0].equals("port2")){
                Assert.assertEquals(ae.getData()[3], "switch1");
            }
            if (ae.getData()[0].equals("port3")){
                Assert.assertEquals(ae.getData()[3], "switch2");
            }
        }
        Assert.assertEquals(4, alerts.size());
    }
}