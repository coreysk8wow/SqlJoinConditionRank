import subprocess
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer

cat = subprocess.Popen(["hadoop", "fs", "-cat", "/user/hive/warehouse/dw_htlapidb.db/tmp_sql_relation_table_joins_for_tfidf/*"], stdout=subprocess.PIPE)
rights = []
lefts = []
for line in cat.stdout:
    lefts.append(line.split(chr(1))[0])
    r_table_arr = line.split(chr(1))[1].split(',')
    new_right = ''
    for table in r_table_arr:
        new_right = new_right + table.strip() + ','
    if len(new_right) > 0:
        rights.append(new_right[0 : -1])

cat.kill()

def split_by_dot_tokenizer(x):
    words = x.split(',')
    return words

cv = CountVectorizer(tokenizer=split_by_dot_tokenizer)
rights_cnt = cv.fit_transform(rights)


tt = TfidfTransformer(smooth_idf=True)
tfidf = tt.fit_transform(rights_cnt)

tfidf_arr = tfidf.toarray()
rights_dict = cv.vocabulary_
rights_list = cv.get_feature_names()
lefts = lefts


result_str_list = []
idx_left = 0
for left in lefts:
    right_tables = rights[idx_left].split(',')
    for r_table in right_tables:
        idx_right = rights_dict.get(r_table)
        line = left + chr(1) + r_table + chr(1) + str(tfidf_arr[idx_left][idx_right])
        result_str_list.append(line)
    idx_left += 1

result_str_set = set(result_str_list)

import os.path
output_file = './sql_relation_tfidf'
if os.path.exists(output_file):
    os.remove(output_file)

f = open('./sql_relation_tfidf', 'w')
for line in result_str_set:
    f.write(line)
    f.write(os.linesep)
f.close()

