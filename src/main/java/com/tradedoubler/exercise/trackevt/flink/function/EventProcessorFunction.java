package com.tradedoubler.exercise.trackevt.flink.function;

import com.tradedoubler.exercise.trackevt.model.TrackEvtApi;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;


@Slf4j
public class EventProcessorFunction extends KeyedProcessFunction<String, TrackEvtApi.TrackingEvent, TrackEvtApi.TrackingResult> {


    private transient ValueState<List<TrackEvtApi.TrackingEvent>> lastStates;

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<List<TrackEvtApi.TrackingEvent>> eventHistoryDescription =
                new ValueStateDescriptor<>(
                        "last-states",
                        TypeInformation.of(new TypeHint<List<TrackEvtApi.TrackingEvent>>() {
                        }));
        lastStates = getRuntimeContext().getState(eventHistoryDescription);
    }

    @Override
    public void processElement(TrackEvtApi.TrackingEvent newEvent, Context context, Collector<TrackEvtApi.TrackingResult> output) throws Exception {


        List<TrackEvtApi.TrackingEvent> eventHistory = getTrackingEventHistory();

        Optional<TrackEvtApi.TrackingEvent> optionalLastEvent = eventHistory.stream()
                .sorted(Comparator.comparing(TrackEvtApi.TrackingEvent::getTimestamp).reversed())
                .findFirst();

        boolean deviceIdHasBeenProcessed = optionalLastEvent.isPresent()
                && optionalLastEvent.get().getType().equals(TrackEvtApi.EventType.CONVERSION);

        boolean deviceIdIsNotProcessAble = newEvent.getType().equals(TrackEvtApi.EventType.CONVERSION)
                && !optionalLastEvent.isPresent();

        if (deviceIdHasBeenProcessed || deviceIdIsNotProcessAble) {
            return;
        }

        boolean isConversionType = newEvent.getType().equals(TrackEvtApi.EventType.CONVERSION);

        if (isConversionType) {
            TrackEvtApi.TrackingResult trackingResult;

            Optional<TrackEvtApi.TrackingEvent> optionalClickEvent = eventHistory.stream()
                    .filter(trackingEvent -> trackingEvent.getType().equals(TrackEvtApi.EventType.CLICK))
                    .sorted(Comparator.comparing(TrackEvtApi.TrackingEvent::getTimestamp))
                    .findFirst();

            boolean eventIsLessThenTimeWindow =
                    TimeUnit.MILLISECONDS.toHours(newEvent.getTimestamp() - optionalLastEvent.get().getTimestamp()) < 12;

            if (eventIsLessThenTimeWindow) {
                TrackEvtApi.TrackingEvent referenceEvent = optionalClickEvent.isPresent() ? optionalClickEvent.get() : optionalLastEvent.get();

                trackingResult = TrackEvtApi.TrackingResult
                        .newBuilder()
                        .setConversionEventId(newEvent.getId())
                        .setReferrerEventId(referenceEvent.getId())
                        .setReferrerType(referenceEvent.getType())
                        .build();
                output.collect(trackingResult);

                log.info("completed device-id : {}  - {}/{} "
                        , newEvent.getDeviceId().toStringUtf8()
                        , referenceEvent.getType()
                        , newEvent.getType());
            }

        }
        eventHistory.add(newEvent);
        lastStates.update(eventHistory);

    }

    private List<TrackEvtApi.TrackingEvent> getTrackingEventHistory() throws java.io.IOException {
        List<TrackEvtApi.TrackingEvent> eventHistory = lastStates.value();

        if (eventHistory == null) {
            eventHistory = new ArrayList<>();
            lastStates.update(eventHistory);
        }
        return eventHistory;
    }
}
