package hw2rg;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Collectors;

// this could be executed with maven exec plugin:
// mvn exec:java -Dexec.mainClass="hw2rg.InputGen" -Dexec.args="input 1 1000"

public class InputGen implements Runnable {
    
    private static final int LINE_MIN_WORDS = 3;
    private static final int LINE_MAX_WORDS = 7;
    private static final int WORD_LEN_FACTOR = 3;
    
    private static byte[][] frags;
    
    static {
        String[] strFrags = {"a", "e", "i", "o", "u",
            "pa", "ne", "mo", "ri", "ku", "da", "ve", "lo", "si", "tu", "ca", "fe", "go", "hi", "zu",
            "pak", "len", "dim", "for", "jut", "has", "rep", "jog", "tip", "muc"};
        frags = Arrays.asList(strFrags).stream().map(String::getBytes)
            .collect(Collectors.toList()).toArray(new byte[strFrags.length][]);
    }
    
    private static String folder;
    private static long size;
    
    private Random rnd = new Random();
    
    public static void main(String... args) {
        if (args.length < 2) {
            System.out.println("Arguments: <folder-name> <num-files> <length>");
            return;
        }
        folder = args[0];
        new File(folder).mkdirs();
        int n = Integer.parseInt(args[1]);
        size = Long.parseLong(args[2]);
        for (int i = 0; i < n; i++) {
            new Thread(new InputGen()).start();
        }
    }
    
    @Override
    public void run() {
        long count = 0;
        String name = folder + "/f" + rnd.nextInt(1000000) + ".txt";
        System.out.println(name + " started...");
        long t = System.currentTimeMillis();
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(name))) {
            while (count < size) {
                count += generateLine(out);
            }
        } catch (IOException e) {
            System.out.println("Error while writing file: " + e);
        }
        System.out.printf("%s generated in %s ms%n", name, System.currentTimeMillis() - t);
    }
    
    private int generateLine(OutputStream out) throws IOException {
        int len = generateWord(out) + 1;
        int w = rnd.nextInt(LINE_MAX_WORDS - LINE_MIN_WORDS + 1) + LINE_MIN_WORDS - 1;
        for (int i = 0; i < w; i++) {
            out.write(' ');
            len += generateWord(out) + 1;
        }
        out.write('\n');
        return len;
    }
    
    private int generateWord(OutputStream out) throws IOException {
        int len = 0;
        do {
            byte[] frag = frags[rnd.nextInt(frags.length)];
            out.write(frag);
            len += frag.length;
        } while (rnd.nextInt(WORD_LEN_FACTOR) > 0);
        return len;
    }
}

