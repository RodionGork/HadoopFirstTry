A = load './input' as line:chararray;
B = foreach A generate flatten(TOKENIZE(line)) as token;
X = filter B by token matches '[a-z]+[^a-z]?';
C = foreach X generate REGEX_EXTRACT(LOWER(token), '([a-z]+)[^a-z]?', 1) as token;
D = filter C by token matches '[a-z]+';
E = group D by $0;
F = foreach E generate group, COUNT($1);
G = order F by $1 DESC;
store G into './output';

