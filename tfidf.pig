A = load 'input-mini' using PigStorage('\n', '-tagFile');
B = foreach A generate REGEX_EXTRACT($0, '\\d+', 0) as fname, flatten(TOKENIZE(LOWER($1))) as token;
C = filter B by $1 matches '[^a-z]*[a-z]+[^a-z]*';
D = group C by (token, fname);
WD_C = foreach D generate group as wd, COUNT($1) as cnt;

E = group WD_C by wd.fname;

TD = foreach (group E all) generate group, COUNT($1) as cnt;
TW_D = foreach E generate group as fname, SUM($1.cnt), TD.cnt;
TW = foreach (group TW_D all) generate group, SUM($1.$1) as cnt;
DUMP TW;
