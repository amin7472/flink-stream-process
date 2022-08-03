package com.tradedoubler.exercise.trackevt.flink.function;

import com.tradedoubler.exercise.trackevt.BaseTest;
import com.tradedoubler.exercise.trackevt.EventTestUtil;
import com.tradedoubler.exercise.trackevt.common.FileUtil;
import com.tradedoubler.exercise.trackevt.model.TrackEvtApi;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EventSinkFunctionTest extends BaseTest {


    @Test
    public void click_conversion_sick_test() throws Exception {
        FileUtil.createFolderIfNotExist(OUTPUT_PATH);

        List<TrackEvtApi.TrackingResult> trackingResults = new ArrayList<>();
        trackingResults.add(EventTestUtil.createEventResult(DEFAULT_REFERRER_EVENT_ID_1, DEFAULT_CONVERSION_EVENT_ID_1, TrackEvtApi.EventType.CLICK));
        trackingResults.add(EventTestUtil.createEventResult(DEFAULT_REFERRER_EVENT_ID_2, DEFAULT_CONVERSION_EVENT_ID_2, TrackEvtApi.EventType.CONVERSION));

        runFlinkExecute(trackingResults, 2);

        TrackEvtApi.TrackingResult fileOutPut1 =
                FileUtil.convertFileToTrackingResult(OUTPUT_PATH, "out_" + DEFAULT_CONVERSION_EVENT_ID_1 + ".pb");
        TrackEvtApi.TrackingResult fileOutPut2 =
                FileUtil.convertFileToTrackingResult(OUTPUT_PATH, "out_" + DEFAULT_CONVERSION_EVENT_ID_2 + ".pb");

        assertEquals(fileOutPut1.getReferrerType(), TrackEvtApi.EventType.CLICK);
        assertEquals(fileOutPut2.getReferrerType(), TrackEvtApi.EventType.CONVERSION);
    }

    private void runFlinkExecute(List<TrackEvtApi.TrackingResult> trackingResults, int numberOfCore) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TrackEvtApi.TrackingResult> streamSource = env.fromCollection(trackingResults);
        env.setParallelism(numberOfCore);
        streamSource
                .addSink(new EventSinkFunction(OUTPUT_PATH));
        env.execute();
    }

}
