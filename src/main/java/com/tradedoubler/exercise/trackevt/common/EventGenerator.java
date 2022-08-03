package com.tradedoubler.exercise.trackevt.common;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import com.tradedoubler.exercise.trackevt.model.TrackEvtApi;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

public class EventGenerator implements Callable {

    private final String inputPath;
    private final int numberOfData;

    public EventGenerator(String inputPath, int numberOfData) {
        this.inputPath = inputPath;
        this.numberOfData = numberOfData;
    }

    @Override
    public Boolean call() throws Exception {
        FileUtil.createFolderIfNotExist(inputPath);
          try {
            List<String> deviceId = new ArrayList<String>();
            for (int i = 0; i < numberOfData; i++) {
                deviceId.add("deviceId_101010101010110" + i);
            }

            List<TrackEvtApi.EventType> actions = new ArrayList<>();
            actions.add(TrackEvtApi.EventType.CLICK);
            actions.add(TrackEvtApi.EventType.IMPRESSION);
            actions.add(TrackEvtApi.EventType.CONVERSION);

            Random random = new Random();

            for (int i = 0; i < numberOfData; i++) {
                Long currentTime = System.currentTimeMillis();
                TrackEvtApi.EventType eventType = actions.get(random.nextInt(actions.size()));
                TrackEvtApi.TrackingEvent trackingEvent = TrackEvtApi.TrackingEvent.newBuilder()
                        .setId(ByteString.copyFromUtf8("Id_101010101010110" + i))
                        .setDeviceId(ByteString.copyFrom(deviceId.get(random.nextInt(deviceId.size())).getBytes()))
                        .setTimestamp(currentTime+5000)
                        .setType(eventType)
                        .setTypeValue(eventType.getNumber())
                        .setUnknownFields(UnknownFieldSet.newBuilder().build())
                        .build();

                File initialFile = new File(inputPath + "/example" + i + ".pb");
                OutputStream outputStream = new FileOutputStream(initialFile);
                trackingEvent.writeDelimitedTo(outputStream);
                Thread.sleep(1);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }
}