USE word;

CREATE EXTERNAL TABLE IF NOT EXISTS SHAKE_EXT (LINE STRING) LOCATION '/sarge/input/shakespear'; 

CREATE TABLE IF NOT EXISTS WORDCOUNT AS 
SELECT words, count(1) from
 (
   SELECT explode(split(line,"\\W+")) as words FROM SHAKE_EXT
 ) L
GROUP BY words;

CREATE TABLE IF NOT EXISTS WORDS_FILE AS 
select word,regexp_extract(filename,'(^.*\//?)(.*)',2) as filename from
(
select filename, word from ( select split(line,"\\W+") as words, INPUT__FILE__NAME as filename from shake_ext) l LATERAL VIEW explode(words)w as word
)l;

CREATE TABLE IF NOT EXISTS INVERT_INDEX AS
select word,collect_set(filename) as filenames from WORDS_FILE group by word;

set num_files=select size(collect_set(distinct filename)) from WORDS_FILE;
set n=${hiveconf:num_files};

CREATE TABLE IF NOT EXISTS TFIDF AS 
select b.word,b.filename,(b.tf*(log10(${hiveconf:n})/a.df)) as tfidf from ( select word,size(collect_set(filename)) as df from WORDS_FILE group by word ) a join (select word,count(1) as tf,filename from WORDS_FILE group by word,filename) b on a.word=b.word;


