package com.tradedoubler.exercise.trackevt.common;

import com.tradedoubler.exercise.trackevt.model.TrackEvtApi;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class FileUtil {


    public static void createFolderIfNotExist(String folderName) throws IOException {
        File folder = new File(folderName);
        if (!folder.exists()) {
            FileUtils.forceMkdir(folder);
        }
    }

    public static TrackEvtApi.TrackingEvent convertFileToTrackingEvent(String inputPath, String fileName) throws IOException {
        File initialFile = new File(inputPath + "/" + fileName);
        InputStream targetStream = new FileInputStream(initialFile);
        return TrackEvtApi.TrackingEvent.parseDelimitedFrom(targetStream);
    }

    public static TrackEvtApi.TrackingResult convertFileToTrackingResult(String inputPath, String fileName) throws IOException {
        File initialFile = new File(inputPath + "/" + fileName);
        InputStream targetStream = new FileInputStream(initialFile);
        return TrackEvtApi.TrackingResult.parseDelimitedFrom(targetStream);
    }

    public static void deleteDirectory(String path) throws IOException {
        FileUtils.cleanDirectory(new File(path));
        FileUtils.delete(new File(path));

    }

}
