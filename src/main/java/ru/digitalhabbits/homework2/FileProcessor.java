package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();

    private ExecutorService executorService = Executors.newFixedThreadPool(CHUNK_SIZE);

    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        checkFileExists(processingFileName);

        final File file = new File(processingFileName);
        Exchanger<List<Future<Pair<String, Integer>>>> exchanger = new Exchanger<>();
        Thread fileWriterThread = new Thread(new FileWriter(resultFileName, exchanger), "FileWriter thread");
        fileWriterThread.start();
        final LineCounterProcessor lineCounterProcessor = new LineCounterProcessor();

        try (final Scanner scanner = new Scanner(file, defaultCharset())) {
            while (scanner.hasNext()) {
                List<String> lines = new ArrayList<>();
                while(scanner.hasNextLine() && lines.size() < CHUNK_SIZE){
                    lines.add(scanner.nextLine());
                }
                List<Future<Pair<String, Integer>>> futures = new ArrayList<>();
                for (String line : lines){
                    Future<Pair<String, Integer>> future = executorService.submit(() -> lineCounterProcessor.process(line));
                    futures.add(future);
                }
                exchanger.exchange(futures);
            }
        } catch (IOException | InterruptedException exception) {
            logger.error("", exception);
        }
        executorService.shutdown();
        try {
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        fileWriterThread.interrupt();

        logger.info("Finish main thread {}", Thread.currentThread().getName());
    }

    private void checkFileExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File '" + fileName + "' not exists");
        }
    }
}
