package hw1rg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class App {
    
    public static void main(String[] args) throws Exception {
        new App().run(args[0], args[1]);
    }

    private void run(String input, String output) throws Exception {
        Path inputPath = new Path(input);
        FileSystem fileSystem = FileSystem.get(inputPath.toUri(), new Configuration());
        List<Path> inputFiles = collectFilePaths(fileSystem, inputPath);
        System.out.println(inputFiles);
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

