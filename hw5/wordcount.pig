InputData = LOAD 'input';

Tokens = foreach InputData generate FLATTEN(TOKENIZE((chararray)$0)) as word;

Tokens = foreach Tokens generate LOWER((chararray)$0) as word;

FilteredData = FILTER Tokens BY (word matches '.*hackathon.*' OR word matches '.*dec.*' OR word matches '.*chicago.*' OR word matches '.*java.*');

Groups = GROUP FilteredData BY word;

DUMP Groups;
