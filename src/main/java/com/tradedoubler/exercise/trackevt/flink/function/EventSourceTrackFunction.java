package com.tradedoubler.exercise.trackevt.flink.function;

import com.tradedoubler.exercise.trackevt.common.FileUtil;
import com.tradedoubler.exercise.trackevt.model.TrackEvtApi;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.nio.file.*;

public class EventSourceTrackFunction implements SourceFunction<TrackEvtApi.TrackingEvent> {

    private final String inputPath;

    private volatile boolean isRunning = true;

    public EventSourceTrackFunction(String inputPath) {
        this.inputPath = inputPath;
    }

    @Override
    public void run(SourceContext<TrackEvtApi.TrackingEvent> sourceContext) throws Exception {
        FileUtil.createFolderIfNotExist(inputPath);

        WatchService watchService = FileSystems.getDefault().newWatchService();
        Path directory = Paths.get(inputPath);
        WatchKey watchKey = directory.register(watchService, StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_DELETE);
        while (isRunning) {
            for (WatchEvent<?> event : watchKey.pollEvents()) {
                WatchEvent<Path> pathEvent = (WatchEvent<Path>) event;
                Path fileName = pathEvent.context();
                WatchEvent.Kind<?> kind = event.kind();
                if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                    TrackEvtApi.TrackingEvent newTrackingEvent = FileUtil.convertFileToTrackingEvent(inputPath, fileName.toString());
                    if (newTrackingEvent != null) {
                        addToSource(newTrackingEvent, sourceContext);
                    }
                }
            }
            boolean valid = watchKey.reset();
            if (!valid) {
                break;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private void addToSource(TrackEvtApi.TrackingEvent newTrackingEvent, SourceContext<TrackEvtApi.TrackingEvent> sourceContext) {
        sourceContext.collect(newTrackingEvent);
    }



}
