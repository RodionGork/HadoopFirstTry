A = load './input-100' as line:chararray;
B = foreach A generate flatten(TOKENIZE(LOWER(line))) as token;
C = filter B by token matches '[^a-z]*[a-z]+[^a-z]*';
D = foreach C generate REGEX_EXTRACT(token, '^[^a-z]*([a-z]+)[^a-z]*$', 1) as token;
E = group D by $0;
F = foreach E generate group, COUNT($1);
G = order F by $1 DESC;
store G into './output-100-pig-2';

