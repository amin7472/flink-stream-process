package com.tradedoubler.exercise.trackevt.flink.function;


import com.tradedoubler.exercise.trackevt.BaseTest;
import com.tradedoubler.exercise.trackevt.EventTestUtil;
import com.tradedoubler.exercise.trackevt.model.TrackEvtApi;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class EventProcessorFunctionTest extends BaseTest {

    @Test
    public void click_conversion_test() throws Exception {

        List<TrackEvtApi.TrackingEvent> trackingEvents = new ArrayList<>();
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_1, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CLICK, 0));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_2, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CONVERSION, 1));

        runFlinkExecute(trackingEvents, 2);

        assertEquals(new EventTestUtil.CollectSink().values.size(), 1);
        assertEquals(new EventTestUtil.CollectSink().values.get(0).getReferrerType(), TrackEvtApi.EventType.CLICK);
    }

    @Test
    public void click_impression_test() throws Exception {

        List<TrackEvtApi.TrackingEvent> trackingEvents = new ArrayList<>();
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_1, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CLICK, 0));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_2, DEFAULT_DEVICE_1, TrackEvtApi.EventType.IMPRESSION, 1));

        runFlinkExecute(trackingEvents, 2);

        assertTrue(new EventTestUtil.CollectSink().values.isEmpty());
    }

    @Test
    public void impression_conversion_test() throws Exception {


        List<TrackEvtApi.TrackingEvent> trackingEvents = new ArrayList<>();
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_2, DEFAULT_DEVICE_1, TrackEvtApi.EventType.IMPRESSION, 1));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_3, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CONVERSION, 2));

        runFlinkExecute(trackingEvents, 2);

        assertEquals(new EventTestUtil.CollectSink().values.get(0).getReferrerType(), TrackEvtApi.EventType.IMPRESSION);
        assertEquals(new EventTestUtil.CollectSink().values.get(0).getConversionEventId().toStringUtf8(), DEFAULT_ID_3);
    }

    @Test
    public void click_impression_conversion_test() throws Exception {


        List<TrackEvtApi.TrackingEvent> trackingEvents = new ArrayList<>();
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_1, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CLICK, 0));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_2, DEFAULT_DEVICE_1, TrackEvtApi.EventType.IMPRESSION, 1));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_3, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CONVERSION, 2));

        runFlinkExecute(trackingEvents, 2);

        assertEquals(new EventTestUtil.CollectSink().values.get(0).getReferrerType(), TrackEvtApi.EventType.CLICK);
        assertEquals(new EventTestUtil.CollectSink().values.get(0).getConversionEventId().toStringUtf8(), DEFAULT_ID_3);
    }


    @Test
    public void impression_click_conversion_test() throws Exception {


        List<TrackEvtApi.TrackingEvent> trackingEvents = new ArrayList<>();
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_1, DEFAULT_DEVICE_1, TrackEvtApi.EventType.IMPRESSION, 0));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_2, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CLICK, 1));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_3, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CONVERSION, 2));

        runFlinkExecute(trackingEvents, 2);

        assertEquals(new EventTestUtil.CollectSink().values.get(0).getReferrerType(), TrackEvtApi.EventType.CLICK);
        assertEquals(new EventTestUtil.CollectSink().values.get(0).getConversionEventId().toStringUtf8(), DEFAULT_ID_3);
    }


    @Test
    public void click_impression_impression_impression_conversion_test() throws Exception {


        List<TrackEvtApi.TrackingEvent> trackingEvents = new ArrayList<>();
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_1, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CLICK, 0));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_2, DEFAULT_DEVICE_1, TrackEvtApi.EventType.IMPRESSION, 1));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_3, DEFAULT_DEVICE_1, TrackEvtApi.EventType.IMPRESSION, 2));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_4, DEFAULT_DEVICE_1, TrackEvtApi.EventType.IMPRESSION, 10));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_5, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CONVERSION, 18));

        runFlinkExecute(trackingEvents, 2);

        assertEquals(new EventTestUtil.CollectSink().values.get(0).getReferrerType(), TrackEvtApi.EventType.CLICK);
        assertEquals(new EventTestUtil.CollectSink().values.get(0).getConversionEventId().toStringUtf8(), DEFAULT_ID_5);
    }

    @Test
    public void click_impression_conversion_multi_device_test() throws Exception {


        List<TrackEvtApi.TrackingEvent> trackingEvents = new ArrayList<>();
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_1, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CLICK, 0));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_2, DEFAULT_DEVICE_1, TrackEvtApi.EventType.IMPRESSION, 1));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_3, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CONVERSION, 2));

        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_1, DEFAULT_DEVICE_2, TrackEvtApi.EventType.CLICK, 0));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_2, DEFAULT_DEVICE_2, TrackEvtApi.EventType.IMPRESSION, 1));


        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_4, DEFAULT_DEVICE_3, TrackEvtApi.EventType.CLICK, 0));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_5, DEFAULT_DEVICE_3, TrackEvtApi.EventType.CONVERSION, 2));

        runFlinkExecute(trackingEvents, 2);
        assertEquals(new EventTestUtil.CollectSink().values.get(0).getReferrerType(), TrackEvtApi.EventType.CLICK);
        assertEquals(new EventTestUtil.CollectSink().values.get(0).getConversionEventId().toStringUtf8(), DEFAULT_ID_3);



        assertEquals(new EventTestUtil.CollectSink().values.get(1).getReferrerType(), TrackEvtApi.EventType.CLICK);
        assertEquals(new EventTestUtil.CollectSink().values.get(1).getConversionEventId().toStringUtf8(), DEFAULT_ID_5);
     }


    @Test
    public void expired_time_windows_rule_conversion_test() throws Exception {

        List<TrackEvtApi.TrackingEvent> trackingEvents = new ArrayList<>();
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_1, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CLICK, 0));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_2, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CONVERSION, 18));

        runFlinkExecute(trackingEvents, 2);

        Assertions.assertTrue(new EventTestUtil.CollectSink().values.isEmpty());
    }

    @Test
    public void time_windows_rule_conversion_test() throws Exception {

        List<TrackEvtApi.TrackingEvent> trackingEvents = new ArrayList<>();
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_1, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CLICK, 0));

        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_2, DEFAULT_DEVICE_1, TrackEvtApi.EventType.IMPRESSION, 13));

        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_3, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CONVERSION, 18));

        runFlinkExecute(trackingEvents, 2);

        assertEquals(new EventTestUtil.CollectSink().values.get(0).getReferrerType(), TrackEvtApi.EventType.CLICK);
        assertEquals(new EventTestUtil.CollectSink().values.get(0).getConversionEventId().toStringUtf8(), DEFAULT_ID_3);
    }


    @Test
    public void only_conversion_test() throws Exception {

        List<TrackEvtApi.TrackingEvent> trackingEvents = new ArrayList<>();
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_1, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CONVERSION, 0));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_1, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CONVERSION, 2));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_1, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CONVERSION, 3));

        runFlinkExecute(trackingEvents, 2);

        Assertions.assertTrue(new EventTestUtil.CollectSink().values.isEmpty());
    }

    @Test
    public void conversion_click_test() throws Exception {

        List<TrackEvtApi.TrackingEvent> trackingEvents = new ArrayList<>();
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_1, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CONVERSION, 0));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_1, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CLICK, 1));

        runFlinkExecute(trackingEvents, 2);

        Assertions.assertTrue(new EventTestUtil.CollectSink().values.isEmpty());

    }


    @Test
    public void repeatedly_device_id() throws Exception {

        List<TrackEvtApi.TrackingEvent> trackingEvents = new ArrayList<>();
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_1, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CLICK, 0));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_2, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CONVERSION, 1));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_1 + 11, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CLICK, 2));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_2 + 12, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CONVERSION, 3));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_1 + 13, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CLICK, 4));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_2 + 14, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CONVERSION, 5));
        trackingEvents.add(EventTestUtil.createEvent(DEFAULT_ID_1 + 15, DEFAULT_DEVICE_1, TrackEvtApi.EventType.CLICK, 6));

        runFlinkExecute(trackingEvents, 2);

        assertEquals(new EventTestUtil.CollectSink().values.size(), 1);
        assertEquals(new EventTestUtil.CollectSink().values.get(0).getReferrerType(), TrackEvtApi.EventType.CLICK);
    }

    private void runFlinkExecute(List<TrackEvtApi.TrackingEvent> trackingEvents, int numberOfCore) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TrackEvtApi.TrackingEvent> streamSource = env.fromCollection(trackingEvents);
        env.setParallelism(numberOfCore);
        streamSource
                .keyBy(trackingEvent -> trackingEvent.getDeviceId().toStringUtf8())
                .process(new EventProcessorFunction())
                .addSink(new EventTestUtil.CollectSink());
        env.execute();
    }
}
