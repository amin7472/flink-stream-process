package com.tradedoubler.exercise.trackevt.flink.function;

import com.tradedoubler.exercise.trackevt.BaseTest;
import com.tradedoubler.exercise.trackevt.EventTestUtil;
import com.tradedoubler.exercise.trackevt.common.EventGenerator;
import com.tradedoubler.exercise.trackevt.model.TrackEvtApi;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.Objects;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class EventSourceTrackFunctionTest extends BaseTest {


    @Test
    public void event_source_track_500_files_created() throws Exception {

        runFlinkJobOnOtherThread();

        ExecutorService executor = Executors.newSingleThreadExecutor();

        Future<Boolean> EventGeneratorFuture = executor.submit(new EventGenerator(INPUT_PATH, 500));

        waitCurrentThreadUntilGeneratorJobFinish(EventGeneratorFuture);

        assertEquals(new EventTestUtil.CollectSink().values.size(), 500);

    }

    private void waitCurrentThreadUntilGeneratorJobFinish(Future<Boolean> eventGeneratorFuture) throws InterruptedException {
        while (!eventGeneratorFuture.isDone()) {
            Thread.sleep(1);
        }
    }

    private void runFlinkJobOnOtherThread() throws InterruptedException {
        CompletableFuture.runAsync(() -> {
            try {
                StreamExecutionEnvironment streamEnv
                        = StreamExecutionEnvironment.getExecutionEnvironment();
                streamEnv.setParallelism(1);

                DataStreamSource<TrackEvtApi.TrackingEvent> streamSource =
                        streamEnv.addSource(new EventSourceTrackFunction(INPUT_PATH));

                streamSource
                        .filter(Objects::nonNull)
                        .map(trackingEvent -> TrackEvtApi.TrackingResult.newBuilder().setReferrerEventId(trackingEvent.getId()).build())
                        .addSink(new EventTestUtil.CollectSink());
                streamEnv.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        //sleep to flink job will be ready
        Thread.sleep(TimeUnit.SECONDS.toMillis(3));
    }


}
