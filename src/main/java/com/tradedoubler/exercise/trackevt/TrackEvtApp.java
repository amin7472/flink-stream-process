package com.tradedoubler.exercise.trackevt;

import com.tradedoubler.exercise.trackevt.common.FileUtil;
import com.tradedoubler.exercise.trackevt.flink.function.EventProcessorFunction;
import com.tradedoubler.exercise.trackevt.flink.function.EventSinkFunction;
import com.tradedoubler.exercise.trackevt.flink.function.EventSourceTrackFunction;
import com.tradedoubler.exercise.trackevt.model.TrackEvtApi;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


@Slf4j
public class TrackEvtApp {


    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        String inputPath = parameters.get("inputPath") == null ? "input" : parameters.get("inputPath");
        String outputPath = parameters.get("outputPath") == null ? "output" : parameters.get("outputPath");

        FileUtil.createFolderIfNotExist(inputPath);
        FileUtil.createFolderIfNotExist(outputPath);


        log.info("\nApp is listen to folder : {} and results will be push to folder : {}\n", inputPath, outputPath);

        final StreamExecutionEnvironment streamEnv
                = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(5);

        DataStreamSource<TrackEvtApi.TrackingEvent> streamSource =
                streamEnv.addSource(new EventSourceTrackFunction(inputPath));

        streamSource
                .keyBy(trackingEvent -> trackingEvent.getDeviceId().toStringUtf8())
                .process(new EventProcessorFunction())
                .addSink(new EventSinkFunction(outputPath));
        streamEnv.execute("Event Streaming job .... ");
    }
}