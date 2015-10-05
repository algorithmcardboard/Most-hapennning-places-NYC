InputData = LOAD 'input';
lines = FOREACH InputData GENERATE LOWER((chararray)$0) as line;
FilteredData = FILTER lines BY (line matches '.*hackathon.*');
DUMP FilteredData;
