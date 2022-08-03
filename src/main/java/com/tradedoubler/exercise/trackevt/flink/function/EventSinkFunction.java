package com.tradedoubler.exercise.trackevt.flink.function;

import com.tradedoubler.exercise.trackevt.model.TrackEvtApi;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

public class EventSinkFunction extends RichSinkFunction<TrackEvtApi.TrackingResult> {
    private final String outputPath;

    public EventSinkFunction(String outputPath) {
        this.outputPath = outputPath;
    }

    @Override
    public void invoke(TrackEvtApi.TrackingResult trackingResult, Context context) throws Exception {
        File initialFile = new File(outputPath + "/out_" + trackingResult.getConversionEventId().toStringUtf8() + ".pb");
        OutputStream outputStream = new FileOutputStream(initialFile);
        trackingResult.writeDelimitedTo(outputStream);
    }


}
