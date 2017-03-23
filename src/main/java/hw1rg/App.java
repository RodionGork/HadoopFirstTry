package hw1rg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class App {

    private static class CompactString {
        
        private byte[] body;
        private int hash;
        
        private CompactString(String s) {
            body = s.getBytes();
            hash = Arrays.hashCode(body);
        }
        
        @Override
        public boolean equals(Object o) {
            return (o instanceof CompactString)
                && Arrays.equals(((CompactString) o).body, body);
        }
        
        @Override
        public int hashCode() {
            return hash;
        }
        
        @Override
        public String toString() {
            return new String(body);
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        new App().run(args[0], args[1]);
    }

    private void run(String input, String output) throws Exception {
        Path inputPath = new Path(input);
        FileSystem fileSystem = FileSystem.get(inputPath.toUri(), new Configuration());
        List<Path> inputFiles = collectFilePaths(fileSystem, inputPath);
        Map<CompactString, AtomicLong> counters = new HashMap<>();
        long skipped = 0;
        long parsed = 0;
        for (Path file : inputFiles) {
            System.out.println("File: " + file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(file)));
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                String[] parts = line.split("\\t");
                if (parts.length != 21) {
                    skipped += 1;
                    continue;
                }
                parsed += 1;
                CompactString id = new CompactString(parts[2]);
                AtomicLong counter = counters.get(id);
                if (counter == null) {
                    counter = new AtomicLong();
                    counters.put(id, counter);
                    if (counters.size() % 10000 == 0) {
                        System.out.println("Counters: " + counters.size());
                    }
                }
                counter.incrementAndGet();
            }
        }
        System.out.printf("Total lines parsed: %s, skipped: %s, unique ids: %s%n", parsed, skipped, counters.size());
        long[] cnts = new long[counters.size()];
        int i = 0;
        for (AtomicLong v : counters.values()) {
            cnts[i] = v.get();
            i += 1;
        }
        Arrays.sort(cnts);
        long threshold = cnts[cnts.length - 100];
        List<Map.Entry<String, Long>> result = new ArrayList<>();
        for (Map.Entry<CompactString, AtomicLong> e : counters.entrySet()) {
            if (e.getValue().get() < threshold) {
                continue;
            }
            result.add(new AbstractMap.SimpleEntry<>(e.getKey().toString(), e.getValue().get()));
        }
        Collections.sort(result, (x, y) -> y.getValue().compareTo(x.getValue()));
        for (Map.Entry<String, Long> e : result) {
            System.out.printf("%s: %s%n", e.getKey(), e.getValue());
        }
    }

    private List<Path> collectFilePaths(FileSystem fileSystem, Path inputPath) throws IOException {
        List<Path> inputFiles = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> it = fileSystem.listFiles(extractOnlyPath(inputPath), true);
        while (it.hasNext()) {
            LocatedFileStatus file = it.next();
            inputFiles.add(extractOnlyPath(file.getPath()));
        }
        return inputFiles;
    }

    private Path extractOnlyPath(Path inputPath) {
        return new Path(inputPath.toUri().getPath());
    }
}

