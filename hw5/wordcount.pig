-- Load the input
InputData = LOAD 'input';

-- Generate word tokens
Tokens = foreach InputData generate TOKENIZE((chararray)$0) as word;
Tokens = foreach Tokens generate FLATTEN(word) as word;
Tokens = foreach Tokens generate FLATTEN(REGEX_EXTRACT_ALL(LOWER(word),'.*?([a-z]+).*?')) as word;

-- Filter Tokens
FilteredData = FILTER Tokens BY (word matches '.*hackathon.*' OR word matches '.*dec.*' OR word matches '.*chicago.*' OR word matches '.*java.*');

-- Group
FilteredData = FILTER Tokens BY (word matches '.*hackathon.*' OR word matches '.*dec.*' OR word matches '.*chicago.*' OR word matches '.*java.*');

Groups = GROUP FilteredData BY word;

Result = foreach Groups generate group, COUNT(FilteredData);

DUMP Result;
