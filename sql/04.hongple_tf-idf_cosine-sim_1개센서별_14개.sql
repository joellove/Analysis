-- =======================================================================================================================================================================
-- 패턴ID별 TF-IDF Score 구하기
-- 패턴 ID의  각 센서값 : Term
-- 패턴 ID : Doc
-- 센서 조합 : 14개
-- =======================================================================================================================================================================

-- 패턴ID에서 Term 추출하기
-- 14개의 센서값으로 구성된 패턴ID에서 한자리씩 빼서 Term 만들기 

delete jar hdfs:///user/vcrm_6442267/udf/analysis.udf-0.0.1-SNAPSHOT.jar;
drop temporary function if exists termgenerator2;

ADD JAR hdfs:///user/vcrm_6442267/udf/analysis.udf-0.0.1-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION termgenerator2 as 'org.apache.hadoop.hive.ql.udtf.TermGeneratorUDTF2';

-- ADD JAR hdfs:///user/vcrm_master/lib/analysis.udf-0.0.1-SNAPSHOT.jar;
-- CREATE TEMPORARY FUNCTION termgenerator as 'org.apache.hadoop.hive.ql.udtf.TermGeneratorUDTF';

drop table if exists vcrm_6442267.rg_log_um_pattern_f10_14_term2;
create table vcrm_6442267.rg_log_um_pattern_f10_14_term2 STORED AS PARQUET
as
SELECT   
 id
 ,ad.term
FROM vcrm_6442267.rg_log_um_pattern_f10_14
lateral view termgenerator2(id) ad as term
;
-- 3928025 (14개)
-- 917179895 (49개)


-- TF-IDF
drop table if exists vcrm_6442267.rg_log_um_pattern_f10_14_term_tfidf2;
create table if not exists vcrm_6442267.rg_log_um_pattern_f10_14_term_tfidf2
(
	doc_name 		string
	, term 			string
	, tfidf 			double
)
row format delimited fields terminated by '\t' 
lines terminated by '\n'
stored as parquet
-- tblproperties ("skip.header.line.count" = "1")
;


insert overwrite table vcrm_6442267.rg_log_um_pattern_f10_14_term_tfidf2
if not exists
select t4.doc_name, t4.term, cast((t4.tf * t4.idf) as double) as tfidf
from 
(
	select t1.doc_name, t1.term, t1.tf as cnt
		, ((0.5 + (0.5 * t1.tf) / t2.max_tf_in_doc)) as tf 	
		, t3.idf as idf
	from 
		(
			select id as doc_name, term, count(*) as tf from vcrm_6442267.rg_log_um_pattern_f10_14_term2
			group by id, term
		) t1 
		join 
		(  	
			select id as doc_name, max(tf) as max_tf_in_doc
			from
			(
				select id, term, count(*) as tf from vcrm_6442267.rg_log_um_pattern_f10_14_term2
		  		group by id, term
		  	) A
		  	group by id
		) t2 on (t1.doc_name = t2.doc_name)
		join
		(
			select tt1.term, tt1.term_cnt_in_all_docs, (1 + log10(tt2.all_documents_cnt / tt1.term_cnt_in_all_docs)) as idf
			from 
			(
				select term, count(*) as term_cnt_in_all_docs from vcrm_6442267.rg_log_um_pattern_f10_14_term2
				group by term
			) tt1 
			cross join 
			(
				select count(distinct id) as all_documents_cnt 
				from vcrm_6442267.rg_log_um_pattern_f10_14_term2
			) tt2
		) t3 on (t1.term = t3.term)
) t4
;
-- 2753984

-- =======================================================================================================================================================================
-- 패턴ID별 Cosine-Similarity 구하기
-- =======================================================================================================================================================================

-- 분자의 값만 따로 구하기 
-- 테이블 생성하고, cosine-similarity paring MR 실행
drop table if exists vcrm_6442267.rg_log_um_pattern_f10_14_numer2;
create table if not exists vcrm_6442267.rg_log_um_pattern_f10_14_numer2
(
	pair_docs		string	
	, numerator		 	double
)
row format delimited fields terminated by '\t' 
lines terminated by '\n'
-- stored as textfile
stored as parquet
-- tblproperties ("skip.header.line.count" = "1")
;

-- cosine-pairing job 실행
#!/usr/bin/env bash

#-Dmapreduce.map.memory.mb=32768 \
#-Dmapreduce.map.java.opts=-Xmx26214 \
#-Dmapreduce.reduce.memory.mb=32768 \
#-Dmapreduce.reduce.java.opts=-Xmx26214 \
#-Dyarn.app.mapreduce.am.resource.mb=32768 \
#-Dyarn.app.mapreduce.am.command-opts=-Xmx26214 \
#-Dmapreduce.task.timeout=86400000 \

#mv ./snippet-hadooop-0.0.1-SNAPSHOT-job.jar ./paring-cosine-term-job.jar

hadoop jar \
./paring-cosine-term-job.jar \
similarity.cosine.mr.term.parquet.ParingDocsTfidf \
-Dmapreduce.job.reduces=100 \
-Dmapreduce.map.output.compress=true \
-Dmapreduce.output.fileoutputformat.compress.type=BLOCK \
-Dmapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec \
-Dmapreduce.input.fileinputformat.split.minsize=268435456 \
/user/hive/warehouse/vcrm_6442267.db/rg_log_um_pattern_f10_14_term_tfidf2 \
/user/hive/warehouse/vcrm_6442267.db/rg_log_um_pattern_f10_14_numer2


-- 분모의 값만 따로 구하기 
drop table if exists vcrm_6442267.rg_log_um_pattern_f10_14_denom2;
create table if not exists vcrm_6442267.rg_log_um_pattern_f10_14_denom2
(
	doc_name 		string
	, denominator 	double
)
row format delimited fields terminated by '\t' 
lines terminated by '\n'
stored as textfile
-- stored as parquet
-- tblproperties ("skip.header.line.count" = "1")
;

insert overwrite table vcrm_6442267.rg_log_um_pattern_f10_14_denom2
if not exists
select doc_name, sqrt(sum(tfidf * tfidf)) as denominator
from vcrm_6442267.rg_log_um_pattern_f10_14_term_tfidf2
group by doc_name
;

--  테이블 생성하고, cosine-similarity MR 실행
drop table if exists vcrm_6442267.rg_log_um_pattern_f10_14_cosine2;
create table if not exists vcrm_6442267.rg_log_um_pattern_f10_14_cosine2
(
	doc_name1			string	
	, doc_name2		 	string
	, similarity		double
)
row format delimited fields terminated by '\t' 
lines terminated by '\n'
-- stored as textfile
stored as parquet
-- tblproperties ("skip.header.line.count" = "1")
;

-- 분산캐시를 위한 파일 병합(디멘젼 테이블)
hadoop fs -getmerge /user/hive/warehouse/vcrm_6442267.db/rg_log_um_pattern_f10_14_denom2/ ./denom2.txt
hadoop fs -put ./denom2.txt /user/vcrm_6442267/dimension

-- cosine-similarity job 실행
#!/usr/bin/env bash

#-Dmapreduce.map.memory.mb=32768 \
#-Dmapreduce.map.java.opts=-Xmx26214 \
#-Dmapreduce.reduce.memory.mb=32768 \
#-Dmapreduce.reduce.java.opts=-Xmx26214 \
#-Dyarn.app.mapreduce.am.resource.mb=32768 \
#-Dyarn.app.mapreduce.am.command-opts=-Xmx26214 \
#-Dmapreduce.task.timeout=86400000 \

#mv ./snippet-hadooop-0.0.1-SNAPSHOT-job.jar ./cosine-similarity-term-job.jar

hadoop jar \
./cosine-similarity-term-job.jar \
similarity.cosine.mr.term.parquet.CosineSimilarity \
-Dmapreduce.map.output.compress=true \
-Dmapreduce.output.fileoutputformat.compress.type=BLOCK \
-Dmapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec \
-Dmapreduce.input.fileinputformat.split.minsize=268435456 \
-Dmapreduce.job.reduces=100 \
/user/hive/warehouse/vcrm_6442267.db/rg_log_um_pattern_f10_14_numer2 \
/user/vcrm_6442267/dimension/denom2.txt \
/user/hive/warehouse/vcrm_6442267.db/rg_log_um_pattern_f10_14_cosine2 \
parquet

-- 39,360,450,616


-- =======================================================================================================================================================================
-- Cosine-Similarity 시각화
-- =======================================================================================================================================================================

-- 노드 생성
drop table if exists vcrm_6442267.rg_log_um_pattern_f10_14_cosine_node;
create table vcrm_6442267.rg_log_um_pattern_f10_14_cosine_node
as
select
	concat(D.vin,',',D.bkdw_cd,',',D.occu_ymd,',',D.occu_ctms,',',D.dtc_id) as id
	, concat(D.vin,',',D.bkdw_cd,',',D.occu_ymd,',',D.occu_ctms,',',D.dtc_id,',',D.cnt) as label
	, D.vin
	, D.bkdw_cd
	, D.occu_ymd
	, D.occu_ctms
	, D.dtc_id
	, D.cnt
	, E.main_sensors
	, case when F.vin is not null then '1'
		else '0'
	  end as flag
from
(
	select
		B.id
		-- , concat(B.id, ',', B.cnt) as label
	from
	(
		select
			A.id as id
			--, sum(A.cnt) as cnt
		from
		(
			select
				doc_name1 as id --, count(*) as cnt
			from vcrm_6442267.rg_log_um_pattern_f10_14_cosine2
			group by doc_name1

			union all

			select
				doc_name2 as id --, count(*) as cnt
			from vcrm_6442267.rg_log_um_pattern_f10_14_cosine2
			group by doc_name2
		) A
		group by A.id
	) B
) C
join (select vin, bkdw_cd, occu_ymd, occu_ctms, dtc_id, cnt from vcrm_6442267.dtc_rt_mnt_svc_um_sigid_stat_14_f10
		where cnt is not null and cnt <= 100
		group by vin, bkdw_cd, occu_ymd, occu_ctms, dtc_id, cnt
	) D 
	on C.id = D.dtc_id
join (select * from vcrm_6442267.dtc_main_sensors) E
	on D.bkdw_cd = E.bkdw_cd and D.dtc_id = E.id and D.cnt = E.pattern_cnt
left outer join vcrm_6442267.rg_log_um_vin_list F -- 운행로그 vin으로 필터링
	on D.vin = F.vin and D.dtc_id = F.id
;
-- 2180, cnt <= 100, dtc

-- 엣지 생성
drop table if exists vcrm_6442267.rg_log_um_pattern_f10_14_cosine_edge;
create table vcrm_6442267.rg_log_um_pattern_f10_14_cosine_edge
as
select
 concat(C.vin,',',C.bkdw_cd,',',C.occu_ymd,',',C.occu_ctms,',',B.doc_name1) as Source
 , concat(D.vin,',',D.bkdw_cd,',',D.occu_ymd,',',D.occu_ctms,',',B.doc_name2) as Target
 , cast(B.similarity as decimal(5,3)) as Weight
 , 'Undirected' as Type
 , cast(B.similarity as decimal(5,3)) as Label
from 
(
	select *
	from
	(
		select * from vcrm_6442267.rg_log_um_pattern_f10_14_cosine2 T
		where T.doc_name1 in 
			(select dtc_id from vcrm_6442267.dtc_rt_mnt_svc_um_sigid_stat_14_f10
				where cnt is not null and cnt <= 100
				group by dtc_id
		    )
		    and similarity >= 0.7
	) A
	where A.doc_name2 in 
		(select dtc_id from vcrm_6442267.dtc_rt_mnt_svc_um_sigid_stat_14_f10
				where cnt is not null and cnt <= 100
				group by dtc_id
		)
) B
join (select vin, bkdw_cd, occu_ymd, occu_ctms, dtc_id from vcrm_6442267.dtc_rt_mnt_svc_um_sigid_stat_14_f10
		where cnt is not null and cnt <= 100
		group by vin, bkdw_cd, occu_ymd, occu_ctms, dtc_id
	) C 
	on B.doc_name1 = C.dtc_id
join (select vin, bkdw_cd, occu_ymd, occu_ctms, dtc_id from vcrm_6442267.dtc_rt_mnt_svc_um_sigid_stat_14_f10
		where cnt is not null and cnt <= 100
		group by vin, bkdw_cd, occu_ymd, occu_ctms, dtc_id
	) D 
	on B.doc_name2 = D.dtc_id
join (select distinct vin from vcrm_6442267.rg_log_um_vin_list) E -- 운행로그 vin으로 필터링
	on C.vin = E.vin
join (select distinct vin from vcrm_6442267.rg_log_um_vin_list) F -- 운행로그 vin으로 필터링
	on D.vin = F.vin
;

-- 32503