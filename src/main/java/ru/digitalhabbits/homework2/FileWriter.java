package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.interrupted;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriter implements Runnable {
    private static final Logger logger = getLogger(FileWriter.class);
    private String resultFile;
    private Exchanger<List<Future<Pair<String, Integer>>>> exchanger;

    public FileWriter(String resultFileName, Exchanger<List<Future<Pair<String, Integer>>>> exchanger) {
        this.resultFile = resultFileName;
        this.exchanger = exchanger;
    }

    @Override
    public void run() {
        logger.info("Started writer thread {}", currentThread().getName());

        try (FileOutputStream fos = new FileOutputStream(resultFile)) {
            while (!interrupted()) {
                for (Future<Pair<String, Integer>> future : exchanger.exchange(null)) {
                    try {
                        Pair<String, Integer> pair = future.get();
                        String result = String.format("%s %s", pair.getKey(), pair.getValue());
                        fos.write(result.getBytes(StandardCharsets.UTF_8));
                        fos.write("\n".getBytes(StandardCharsets.UTF_8));
                    } catch (InterruptedException | ExecutionException | IOException e) {
                        e.printStackTrace();
                    }
                }
            }

        } catch (InterruptedException | IOException e) {
            logger.error("", e);
        }

        logger.info("Finish writer thread {}", currentThread().getName());
    }
}
