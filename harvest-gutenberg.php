<?php

$start = 40000;
$n = 10;

@mkdir('input');

$cnt = 0;

for ($i = 0; $cnt < $n; $i++) {
    $idx = $start + $i;
    $text = @file_get_contents("http://www.gutenberg.org/cache/epub/${idx}/pg${idx}.txt");
    if (!$text) {
        echo "Missing {$idx}\n";
    }
    $matches = array();
    if (preg_match('/START OF TH.{1,2} PROJECT.*?\n(.*)END OF TH.{1,2} PROJECT/is', $text, $matches)) {
        $text = $matches[1];
        file_put_contents("input/book{$idx}.txt", $text);
        $len = strlen($text);
        echo "Written {$idx} - {$len}\n";
        $cnt += 1;
    } else {
        echo "Skipped {$idx}\n";
    }
}
