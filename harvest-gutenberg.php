<?php

echo "\nUSAGE: php -f harvest-gutenberg.php [amount=10] [start=10000] [dir=input] [english-only]\n\n";

$start = rand(10000, 50000);
$n = 10;
$engOnly = false;
$folder = 'input';

parseCmdLine();

@mkdir($folder);

$cnt = 0;

for ($i = 0; $cnt < $n; $i++) {
    $idx = $start + $i;
    $text = @file_get_contents("http://www.gutenberg.org/ebooks/{$idx}.txt.utf-8");
    if (!$text) {
        echo "[----] Missing {$idx}\n";
        continue;
    }
    if (strlen($text) >= 3 && $text[0] == "\x1F" && $text[1] == "\x8B" && $text[2] == "\x08") {
        echo '[gzip] ';
        $text = gzdecode($text);
    } else {
        echo '[text] ';
    }
    $text = preg_replace('/\R+/u', "\n", $text);
    if (!preg_match('/START OF (?:THIS|THE) PROJECT GUTENBERG[^\n]*\n+(.*)/isu', $text, $matches)) {
        echo "Skipped {$idx} - no start\n";
        continue;
    }
    $text = $matches[1];
    if (!preg_match('/\n+[^\n]*END OF (?:THIS|THE) PROJECT GUTENBERG.*/isu', $text, $matches)) {
        echo "Skipped {$idx} - no end\n";
        continue;
    }
    $text = substr($text, 0, - strlen($matches[0]));
    if ($engOnly && substr_count(strtolower($text), ' the ') < 10) {
        echo "Skipped {$idx} - not in English\n";
        continue;
    }
    file_put_contents("$folder/book{$idx}.txt", $text);
    $len = strlen($text);
    echo "Written {$idx} - {$len}\n";
    $cnt += 1;
}

function parseCmdLine() {
    global $argv, $n, $start, $engOnly, $folder;
    $matches = array();
    for ($i = 1; $i < count($argv); $i++) {
        if (preg_match('/amount\=(\d+)/', $argv[$i], $matches)) {
            $n = intval($matches[1]);
        }
        if (preg_match('/start\=(\d+)/', $argv[$i], $matches)) {
            $start = intval($matches[1]);
        }
        if (preg_match('/dir\=(\S+)/', $argv[$i], $matches)) {
            $folder = $matches[1];
        }
        if (preg_match('/english-only/', $argv[$i], $matches)) {
            $engOnly = true;
        }
    }
}

