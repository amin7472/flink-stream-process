package com.tradedoubler.exercise.trackevt;

import com.tradedoubler.exercise.trackevt.common.FileUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.IOException;

public class BaseTest {
    public static final String DEFAULT_DEVICE_1 = "Device_100000000001";
    public static final String DEFAULT_DEVICE_2 = "Device_100000000002";

    public static final String DEFAULT_DEVICE_3 = "Device_100000000003";

    public static final String DEFAULT_ID_1 = "Id_1000000000001";
    public static final String DEFAULT_ID_2 = "Id_1000000000002";
    public static final String DEFAULT_ID_3 = "Id_1000000000003";
    public static final String DEFAULT_ID_4 = "Id_1000000000003";
    public static final String DEFAULT_ID_5 = "Id_1000000000003";

    public static final String OUTPUT_PATH = "test-output";
    public static final String INPUT_PATH = "test-input";

    public static final String DEFAULT_REFERRER_EVENT_ID_1 = "R_1000000000001";
    public static final String DEFAULT_REFERRER_EVENT_ID_2 = "R_1000000000002";

    public static final String DEFAULT_CONVERSION_EVENT_ID_1 = "CO_1000000000001";
    public static final String DEFAULT_CONVERSION_EVENT_ID_2 = "CO_1000000000002";


    @AfterEach
    public void afterSetup() throws IOException {
        FileUtil.deleteDirectory(INPUT_PATH);
        FileUtil.deleteDirectory(OUTPUT_PATH);
    }

    @BeforeEach
    public void beforeSetup() throws IOException {
        FileUtil.createFolderIfNotExist(INPUT_PATH);
        FileUtil.createFolderIfNotExist(OUTPUT_PATH);
        new EventTestUtil.CollectSink().values.clear();
    }
}
