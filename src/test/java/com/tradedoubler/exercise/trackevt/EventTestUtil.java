package com.tradedoubler.exercise.trackevt;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import com.tradedoubler.exercise.trackevt.model.TrackEvtApi;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class EventTestUtil {

    public static TrackEvtApi.TrackingEvent createEvent(String id, String deviceId, TrackEvtApi.EventType eventType, long time) {
        return TrackEvtApi.TrackingEvent.newBuilder()
                .setId(ByteString.copyFromUtf8(id))
                .setDeviceId(ByteString.copyFromUtf8(deviceId))
                .setTimestamp(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(time))
                .setType(eventType)
                .setTypeValue(eventType.getNumber())
                .setUnknownFields(UnknownFieldSet.newBuilder().build())
                .build();
    }


    public static TrackEvtApi.TrackingResult createEventResult(String referrerEventId, String conversionEventId,
                                                               TrackEvtApi.EventType eventType) {
        return TrackEvtApi.TrackingResult.newBuilder()
                .setReferrerEventId(ByteString.copyFromUtf8(referrerEventId))
                .setConversionEventId(ByteString.copyFromUtf8(conversionEventId))
                .setReferrerType(eventType)
                .setReferrerTypeValue(eventType.getNumber())
                .setUnknownFields(UnknownFieldSet.newBuilder().build())
                .build();
    }

    public static class CollectSink implements SinkFunction<TrackEvtApi.TrackingResult> {
        public static final List<TrackEvtApi.TrackingResult> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(TrackEvtApi.TrackingResult value) throws Exception {
            values.add(value);
        }
    }
}
