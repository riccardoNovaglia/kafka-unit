package info.batey.kafka.unit.utils;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.commons.io.FileUtils.deleteDirectory;

public class FileUtils {
    private static final Logger LOGGER = Logger.getLogger(FileUtils.class);

    private static List<File> directoriesToDelete = new ArrayList<>();

    static {
        Runtime.getRuntime().addShutdownHook(new DirectoryCleaner());
    }

    public static void registerDirectoriesToDelete(File... directories) {
        directoriesToDelete.addAll(asList(directories));
    }

    private static class DirectoryCleaner extends Thread {
        @Override
        public void run() {
            for (File directory : directoriesToDelete) {
                try {
                    deleteDirectory(directory);
                } catch (IOException e) {
                    LOGGER.warn("Problems deleting temporary directory " + directory.getAbsolutePath(), e);
                }
            }
        }
    }

}
