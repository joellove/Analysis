-- =======================================================================================================================================================================
-- 패턴ID별 주영향센서 추출하기
-- 센서 조합 : 14개
-- 패턴 ID에서 14 - 1 조합 : Term
-- 패턴 ID : Doc
-- =======================================================================================================================================================================

-- 패턴ID에서 Term 추출하기
-- 14개의 센서값으로 구성된 패턴ID에서 한자리씩 뺀 나머지 13개로 Term 만들기 

-- delete jar hdfs:///user/vcrm_6442267/udf/analysis.udf-0.0.1-SNAPSHOT.jar;
-- drop temporary function if exists termgenerator;
-- ADD JAR hdfs:///user/vcrm_6442267/udf/analysis.udf-0.0.1-SNAPSHOT.jar;

delete jar hdfs:///user/vcrm_master/lib/analysis.udf-0.0.1-SNAPSHOT.jar;
drop temporary function if exists termgenerator;

ADD JAR hdfs:///user/vcrm_master/lib/analysis.udf-0.0.1-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION termgenerator as 'org.apache.hadoop.hive.ql.udtf.TermGeneratorUDTF';

drop table if exists vcrm_6442267.rg_log_um_pattern_f10_14_term;
create table vcrm_6442267.rg_log_um_pattern_f10_14_term STORED AS PARQUET
as
SELECT   
 id -- 
 ,concat(ad.seq,'S',ad.term) as term
FROM vcrm_6442267.rg_log_um_pattern_f10_14 -- 
lateral view termgenerator(id) ad as seq, term
;
-- 

-- TF-IDF
drop table if exists vcrm_6442267.rg_log_um_pattern_f10_14_term_tfidf;
create table if not exists vcrm_6442267.rg_log_um_pattern_f10_14_term_tfidf
(
	doc_name 		string -- 18717957
	, term 			string
	, tfidf 			double
)
row format delimited fields terminated by '\t' 
lines terminated by '\n'
stored as parquet
-- tblproperties ("skip.header.line.count" = "1")
;

insert overwrite table vcrm_6442267.rg_log_um_pattern_f10_14_term_tfidf
if not exists
select t4.doc_name, t4.term, cast((t4.tf * t4.idf) as double) as tfidf
from 
(
	select t1.doc_name, t1.term, t1.tf as cnt
		, ((0.5 + (0.5 * t1.tf) / t2.max_tf_in_doc)) as tf 	
		, t3.idf as idf
	from 
		(
			select id as doc_name, term, count(*) as tf from vcrm_6442267.rg_log_um_pattern_f10_14_term
			group by id, term
		) t1 
		join 
		(  	
			select id as doc_name, max(tf) as max_tf_in_doc
			from
			(
				select id, term, count(*) as tf from vcrm_6442267.rg_log_um_pattern_f10_14_term
		  		group by id, term
		  	) A
		  	group by id
		) t2 on (t1.doc_name = t2.doc_name)
		join
		(
			select tt1.term, tt1.term_cnt_in_all_docs, (1 + log10(tt2.all_documents_cnt / tt1.term_cnt_in_all_docs)) as idf
			from 
			(
				select term, count(*) as term_cnt_in_all_docs from vcrm_6442267.rg_log_um_pattern_f10_14_term
				group by term
			) tt1 
			cross join 
			(
				select count(distinct id) as all_documents_cnt 
				from vcrm_6442267.rg_log_um_pattern_f10_14_term
			) tt2
		) t3 on (t1.term = t3.term)
) t4
;
-- 

-- 패턴별 주영향센서 추출
drop table if exists vcrm_6442267.B;
create table vcrm_6442267.B
as
select
	C.doc_name
	, C.tfidf
	, C.sensor_indexes
	, D.cnt
	, D.pct
from 
(
	select
	 A.doc_name
	 , A.tfidf
	 , collect_list(split(A.term,'[S]')[0]) as sensor_indexes
	from vcrm_6442267.rg_log_um_pattern_f10_14_term_tfidf A
	join 
		(
		 select doc_name, min(tfidf) as tfidf
		 from vcrm_6442267.rg_log_um_pattern_f10_14_term_tfidf
		 group by doc_name
		) B
		on A.doc_name = B.doc_name and A.tfidf = B.tfidf
	group by A.doc_name, A.tfidf
) C
left outer join vcrm_6442267.rg_log_um_pattern_f10_14 D
	on C.doc_name = D.id
;
-- 

-- DTC + cnt + 주영향센서
drop table if exists vcrm_6442267.dtc_rt_mnt_svc_um_sigid_stat_14_A;
create table vcrm_6442267.dtc_rt_mnt_svc_um_sigid_stat_14_A
as
select
 A.ems11_n,A.ems11_vs,A.ems11_swi_igk,A.ems11_puc_stat,A.ems12_pv_av_can,A.ems12_temp_eng,A.ems14_amp_can
 ,A.ems_h12_cf_ems_isgstat,A.ems16_eng_stat,A.ems16_tqi_target,A.ems16_tqi,A.tcu11_n_tc,A.tcu11_swi_cc
 ,A.tcu12_cur_gr
 , B.dtc_id as id
 , B.cnt
 , B.pct
 , C.sensor_indexes as p_sensor
 , C.tfidf as p_sensor_tfidf
from (select * from vcrm_6442267.dtc_rt_mnt_um_id where bkdw_cd like 'P%') A
left outer join vcrm_6442267.dtc_rt_mnt_svc_um_sigid_stat_14_f10 B
 on A.vin = B.vin 
	 and A.bkdw_cd = B.bkdw_cd 
	 and A.occu_ymd = B.occu_ymd 
	 and A.occu_ctms = B.occu_ctms
	 and A.frame_no = B.frame_no
left outer join vcrm_6442267.B C
 on B.dtc_id = C.doc_name
;
-- 241833
-- , id is not null

-- DTC코드별 영향센서 건수체크용 
drop table if exists vcrm_6442267.dtc_bkdw_stat2;
create table vcrm_6442267.dtc_bkdw_stat2
as
select
  A.id
  ,	A.bkdw_cd
  , A.cnt as pattern_cnt
  , case when B.sensor_idx = "1"  then "ems11_n"
		 when B.sensor_idx = "2"  then "ems11_vs"
		 when B.sensor_idx = "3"  then "ems11_swi_igk"
		 when B.sensor_idx = "4"  then "ems11_puc_stat"
		 when B.sensor_idx = "5"  then "ems12_pv_av_can"
		 when B.sensor_idx = "6"  then "ems12_temp_eng"
		 when B.sensor_idx = "7"  then "ems14_amp_can"
		 when B.sensor_idx = "8"  then "ems_h12_cf_ems_isgstat"
		 when B.sensor_idx = "9"  then "ems16_eng_stat"
		 when B.sensor_idx = "10" then "ems16_tqi_target"
		 when B.sensor_idx = "11" then "ems16_tqi"
		 when B.sensor_idx = "12" then "tcu11_n_tc"
		 when B.sensor_idx = "13" then "tcu11_swi_cc"
		 when B.sensor_idx = "14" then "tcu12_cur_gr"
	  end as sensor_name
	, count(*) as cnt
from
(
	select  bkdw_cd, dtc_id as id, cnt
	from vcrm_6442267.dtc_rt_mnt_svc_um_sigid_stat_14_f10 
	where cnt is not null and cnt <= 100
) A
join
(
	select
	 A.doc_name
	 , split(A.term,'[S]')[0] as sensor_idx
	from vcrm_6442267.rg_log_um_pattern_f10_14_term_tfidf A
	join 
		(
		 select doc_name, min(tfidf) as tfidf
		 from vcrm_6442267.rg_log_um_pattern_f10_14_term_tfidf
		 group by doc_name
		) B
		on A.doc_name = B.doc_name and A.tfidf = B.tfidf
) B
on A.id = B.doc_name
group by A.id, A.bkdw_cd, A.cnt, B.sensor_idx
;
-- 2885

-- 패턴ID, DTC코드별 주영향센서들
drop table if exists vcrm_6442267.dtc_main_sensors;
create table vcrm_6442267.dtc_main_sensors
as
select
	id, bkdw_cd, pattern_cnt, cnt
	,collect_list(sensor_name) as main_sensors
from vcrm_6442267.dtc_bkdw_stat2
where pattern_cnt is not null and pattern_cnt <= 100
group by id, bkdw_cd, pattern_cnt, cnt
;
-- 1906

-- DTC 이상패턴 체크용, 중복데이터 제거
drop table if exists vcrm_6442267.dtc_rt_mnt_um_id_14;
create table vcrm_6442267.dtc_rt_mnt_um_id_14
as
select
  B.id
  , B.cnt
  , A.*
from
(
select 	
 vin,sale_vehl_cd,bkdw_cd,dgn_sn,dgn_scn_cd,occu_ymd,occu_ctms,svc_data_sn,frame_no
 ,ems11_n,ems11_vs,ems11_swi_igk,ems11_puc_stat,ems12_pv_av_can,ems12_temp_eng,ems14_amp_can
 ,ems_h12_cf_ems_isgstat,ems16_eng_stat,ems16_tqi_target,ems16_tqi,tcu11_n_tc,tcu11_swi_cc,tcu12_cur_gr
from vcrm_5314777.dtc_rt_mnt_servicedata_2g2c_UM A
where bkdw_cd like 'P%' -- P코드만
group by  vin,sale_vehl_cd,bkdw_cd,dgn_sn,dgn_scn_cd,occu_ymd,occu_ctms,svc_data_sn,frame_no
 ,ems11_n,ems11_vs,ems11_swi_igk,ems11_puc_stat,ems12_pv_av_can,ems12_temp_eng,ems14_amp_can
 ,ems_h12_cf_ems_isgstat,ems16_eng_stat,ems16_tqi_target,ems16_tqi,tcu11_n_tc,tcu11_swi_cc,tcu12_cur_gr
) A
join vcrm_6442267.dtc_rt_mnt_svc_um_sigid_stat_14_f10 B
	on A.vin = B.vin and A.bkdw_cd = B.bkdw_cd and A.occu_ymd = B.occu_ymd
    	and A.occu_ctms = B.occu_ctms and A.frame_no = B.frame_no
join 
  (
  	select 
  		T3.vin, T3.bkdw_cd, T3.occu_ymd, T3.occu_ctms
  	from
  	(
  		select T1.* 
  		from (select * from vcrm_6442267.dtc_rt_mnt_svc_um_sigid_stat_14_f10 
  				where cnt is not null and cnt <= 100) T1
    	join vcrm_6442267.rg_log_um_vin_list T2  -- 운행로그 vin으로 필터링
			on T1.id = T2.id
	) T3
	group by T3.vin, T3.bkdw_cd, T3.occu_ymd, T3.occu_ctms

  ) C
	on A.vin = C.vin and A.bkdw_cd = C.bkdw_cd and A.occu_ymd = C.occu_ymd and A.occu_ctms = C.occu_ctms
;
-- 103,892

-- 운행로그 이상패턴 체크용
drop table if exists vcrm_6442267.rg_log_um_id_14;
create table vcrm_6442267.rg_log_um_id_14
as
select 
  A.vin,A.ignitiontime,A.t,A.id,A.cnt
 , row_number() over(partition by A.vin, A.ignitiontime order by A.t) as no
 ,A.ems11_n,A.ems11_vs,A.ems11_swi_igk,A.ems11_puc_stat,A.ems12_pv_av_can,A.ems12_temp_eng,A.ems14_amp_can
 ,A.ems_h12_cf_ems_isgstat,A.ems16_eng_stat,A.ems16_tqi_target,A.ems16_tqi,A.tcu11_n_tc,A.tcu11_swi_cc
 ,A.tcu12_cur_gr
from vcrm_6442267.rg_log_um_f10_factor_cnt A
join 
	(
		select T.vin, T.ignitiontime 
		from vcrm_6442267.rg_log_um_f10_factor_cnt T
		join (select vin, id from vcrm_6442267.dtc_rt_mnt_svc_um_sigid_stat_14_f10 
						where cnt is not null and cnt <= 100
						group by vin, id) T2
			on T.vin = T2.vin -- and T.id = T2.vin
		group by T.vin, T.ignitiontime
	) B
	on A.vin = B.vin and A.ignitiontime = B.ignitiontime
;
-- 281,251
-- 16449188

