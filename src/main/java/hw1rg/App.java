package hw1rg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class App {

    private static final int TOP_RESULTS_COUNT = 100;
    private static final int RECORDS_COUNT_REPORT_INTERVAL = 100000;
    private static final int RECORD_FIELD_COUNT = 21;
    private static final int RECORD_ID_FIELD_POS = 2;

    public static void main(String[] args) {
        long t = System.currentTimeMillis();
        new App().run(args[0], args[1]);
        System.out.printf("Time taken %.3f seconds%n", (System.currentTimeMillis() - t) / 1000.0);
    }

    private void run(String input, String output) {
        Path inputPath = new Path(input);
        FileSystem inputFileSystem = getFileSystemForPath(inputPath);
        List<Path> inputFiles = collectFilePaths(inputFileSystem, inputPath);
        Map<String, AtomicLong> counters = countForAllFiles(inputFileSystem, inputFiles);
        writeTopCounters(output, retrieveTopCounters(counters));
    }

    private void writeTopCounters(String output, List<Map.Entry<String, Long>> result) {
        Path outputPath = new Path(output);
        FileSystem outputFileSystem = getFileSystemForPath(outputPath);
        try (PrintWriter writer = new PrintWriter(outputFileSystem.create(outputPath, true))) {
            for (Map.Entry<String, Long> e : result) {
                writer.printf("%s\t%s%n", e.getKey(), e.getValue());
            }
        } catch (IOException e) {
            throw new RuntimeException("Error while writing results to " + output, e);
        }
        System.out.println("Results were successfully saved to: " + output);
    }

    private List<Map.Entry<String, Long>> retrieveTopCounters(Map<String, AtomicLong> counters) {
        List<Map.Entry<String, Long>> result = counters.entrySet().stream()
                .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue().get())).collect(Collectors.toList());
        result.sort((x, y) -> Long.compare(y.getValue(), x.getValue()));
        return result.subList(0, TOP_RESULTS_COUNT);
    }

    private FileSystem getFileSystemForPath(Path inputPath) {
        try {
            return FileSystem.get(inputPath.toUri(), new Configuration());
        } catch (IOException e) {
            throw new RuntimeException("Error preparing filesystem object for path: " + inputPath, e);
        }
    }

    private Map<String, AtomicLong> countForAllFiles(FileSystem fileSystem, List<Path> inputFiles) {
        int threads = determineThreadNumber();
        System.out.println("Threads: " + threads);
        Map<String, AtomicLong> counters = threads > 1 ? new ConcurrentHashMap<>() : new HashMap<>();
        ExecutorService service = Executors.newFixedThreadPool(threads);
        for (Path file : inputFiles) {
            service.submit(() -> {
                processOneFile(fileSystem, file, counters);
            });
        }
        service.shutdown();
        try {
            service.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            System.out.println("Interrupted...");
        }
        System.out.printf("Total files processed: %s, unique ids: %s%n", inputFiles.size(), counters.size());
        return counters;
    }

    private void processOneFile(FileSystem fileSystem, Path file, Map<String, AtomicLong> counters) {
        System.out.println("Parsing file: " + file);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(file)))) {
            countForOneFile(counters, reader);
        } catch (IOException e) {
            throw new RuntimeException("Error while reading " + file, e);
        }
    }

    private int determineThreadNumber() {
        String prop = System.getProperty("multithreaded");
        if ("".equals(prop)) {
            return Runtime.getRuntime().availableProcessors();
        } else {
            try {
                return Integer.parseInt(prop);
            } catch (NumberFormatException | NullPointerException e) {
                return 1;
            }
        }
    }

    private void countForOneFile(Map<String, AtomicLong> counters, BufferedReader reader) throws IOException {
        long lines = 0;
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            String[] parts = line.split("\\t");
            if (parts.length != RECORD_FIELD_COUNT) {
                continue;
            }
            lines += 1;
            String id = parts[RECORD_ID_FIELD_POS];
            AtomicLong counter = counters.computeIfAbsent(id, k -> new AtomicLong());
            counter.incrementAndGet();
            if (lines % RECORDS_COUNT_REPORT_INTERVAL == 0) {
                System.out.printf("lines read: %d, unique ids total: %d%n", lines, counters.size());
            }
        }
    }

    private List<Path> collectFilePaths(FileSystem fileSystem, Path inputPath) {
        List<Path> inputFiles = new ArrayList<>();
        try {
            RemoteIterator<LocatedFileStatus> it = fileSystem.listFiles(extractOnlyPath(inputPath), true);
            while (it.hasNext()) {
                LocatedFileStatus file = it.next();
                inputFiles.add(extractOnlyPath(file.getPath()));
            }
        } catch (IOException e) {
            throw new RuntimeException("Error while enumerating input files", e);
        }
        return inputFiles;
    }

    private Path extractOnlyPath(Path inputPath) {
        return new Path(inputPath.toUri().getPath());
    }
}

