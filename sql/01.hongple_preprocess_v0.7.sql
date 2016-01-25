## 빅데이터기반 이상패턴분석
## 전처리
## created by Joel Lee
## since 2015.7.22
## 대상 데이터 : 주기적운행로그(2014.09.28~2015.08.02, DM/UM/TL)


## 주기적 운행로그 수집주기 변경 vin 리스트
## 대상 : UM 400대 (2.0, 2.2 각 200대), AG 100대 (첨부 엑셀 참조) 
## 변경 주기 : 월 1회  일 1회
## 기간 : 15년 1월 15일 ~ 3월 4일
create table vcrm_6442267.drv_rg_log_ch_vin(
  vin string,
  qm_vehl_cd string,
  etc string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
STORED AS TEXTFILE
;

## DM/UM/TL(주기운행로그) 
## 7월 31일 기준

select count(*) from vcrm_6442267.rg_log_um_dm_tl
-- 1775296356
select count(*) from hkmc_tms.drv_rg_log_ps_avn4_rc
-- 1677419856

select 
 sale_vehl_cd, min(t) as min, max(t) as max, count(*) as cnt
from drv_rt_log_ps_gis_wheather_accel_avn4_rc_mstr_final 
group by sale_vehl_cd
;
  sale_vehl_cd  min max cnt
0 UM  20140917142920  20150501004921  2938837671
1 KH  20141218061654  20150430231545  23001294
2 TL  20150331210838  20150501012648  68742290
3 DM  20141031044509  20150501013818  2952372300
4 AG  20141108085740  20150501004341  349748970


select 
 sale_vehl_cd, min(t) as min, max(t) as max, count(*) as cnt, count(distinct vin)
from vcrm_6442267.rg_log_um_dm_tl
where substr(ignitiontime, 1, 6) > '201406' and substr(ignitiontime, 1, 6) < '201508'
group by sale_vehl_cd
;
  sale_vehl_cd  min(t) max(t) cnt count(distinct vin)
 UM  20141001055302  20150801005122  626527107 10189
 DM  20141102020436  20150801003630  746119336 11999
 TL  20150404181150  20150801005747  282803565 5237

## NULL 센서군 : 
-- ems19, ems20_cf_ems_pumptpres: 수집은 되나 DM에서 전부 NULL

select 
 sale_vehl_cd, min(t) as min, max(t) as max, count(*) as cnt
from hkmc_tms.drv_rt_log_ps_avn4_rc
where substr(ignitiontime, 1, 6) > '201406' and substr(ignitiontime, 1, 6) < '201508'
group by sale_vehl_cd
;

  sale_vehl_cd  min max cnt
0 AD  20150709102009  20150709195817  26293
1 AG  20141108085740  20150729215243  834386472
2 DM  20141031044509  20150729230204  6948728911
3 GF  20150723050933  20150729232453  4164335
4 KF  20150723060153  20150729223441  5606903
5 KH  20141218061654  20150729174548  114627753
6 LQ  20150608065803  20150728211432  390091
7 TL  20150331210838  20150729225708  1693839123
8 UM  20140917142920  20150729234423  6527157365



## DM/TL/UM 공통센서 추출
drop table if exists vcrm_6442267.rg_log_um_dm_tl;
create table vcrm_6442267.rg_log_um_dm_tl STORED AS PARQUET
as
select
A.*
from
(
  select
  vin, sale_vehl_cd, ignitiontime, t, ems9_cf_ems_brkreq as ems19_cf_ems_brkreq,ems9_cf_ems_dnshftreq as ems19_cf_ems_dnshftreq,ems9_cf_ems_decelreq as ems19_cf_ems_decelreq,ems9_cr_ems_bstpre as ems19_cr_ems_bstpre,ems9_cr_ems_engoiltemp as ems19_cr_ems_engoiltemp,ems9_cf_ems_modeledambtemp as ems19_cf_ems_modeledambtemp,ems9_cf_ems_opsfail as ems19_cf_ems_opsfail,ems6_tqi_min as ems16_tqi_min,ems6_tqi as ems16_tqi,ems6_tqi_target as ems16_tqi_target,ems6_tqi_max as ems16_tqi_max,ems6_cruise_lamp_s as ems16_cruise_lamp_s,ems6_pre_fuel_cut_in as ems16_pre_fuel_cut_in,ems6_eng_stat as ems16_eng_stat,ems6_soak_time_error as ems16_soak_time_error,ems6_soak_time as ems16_soak_time,ems6_spk_time_cur as ems16_spk_time_cur,ems6_cf_ems_aclact as ems16_cf_ems_aclact,fatc_cr_fatc_tqacnout as fatc11_cr_fatc_tqacnout,fatc_cf_fatc_acnrqswi as fatc11_cf_fatc_acnrqswi,fatc_cf_fatc_acncltenrq as fatc11_cf_fatc_acncltenrq,fatc_cf_fatc_ecvflt as fatc11_cf_fatc_ecvflt,fatc_cf_fatc_blwron as fatc11_cf_fatc_blwron,fatc_cf_fatc_blwrmax as fatc11_cf_fatc_blwrmax,fatc_cf_fatc_engstartreq as fatc11_cf_fatc_engstartreq,fatc_cf_fatc_isgstopreq as fatc11_cf_fatc_isgstopreq,fatc_cr_fatc_outtemp as fatc11_cr_fatc_outtemp,fatc_cr_fatc_outtempsns as fatc11_cr_fatc_outtempsns,fatc_cf_fatc_compload as fatc11_cf_fatc_compload,fatc_cf_fatc_activeeco as fatc11_cf_fatc_activeeco,fatc_cf_fatc_autoactivation as fatc11_cf_fatc_autoactivation,ems_h2_r_tqacnapvc as ems_h12_r_tqacnapvc,ems_h2_r_pacnc as ems_h12_r_pacnc,ems_h2_tqi_b as ems_h12_tqi_b,ems_h2_cf_cdastat as ems_h12_cf_cdastat,ems_h2_cf_ems_isgstat as ems_h12_cf_ems_isgstat,ems_h2_cf_ems_oilchg as ems_h12_cf_ems_oilchg,ems_h2_cf_ems_etclimpmod as ems_h12_cf_ems_etclimpmod,ems_h2_r_nengidltgc as ems_h12_r_nengidltgc,ems_h2_cf_ems_uptargr as ems_h12_cf_ems_uptargr,ems_h2_cf_ems_downtargr as ems_h12_cf_ems_downtargr,ems_h2_cf_ems_descurgr as ems_h12_cf_ems_descurgr,ems_h2_cf_ems_hpresstat as ems_h12_cf_ems_hpresstat,ems_h2_cf_ems_fcopen as ems_h12_cf_ems_fcopen,ems_h2_cf_ems_actecoact as ems_h12_cf_ems_actecoact,ems_h2_cf_ems_engrunnorm as ems_h12_cf_ems_engrunnorm,ems_h2_cf_ems_isgstat2 as ems_h12_cf_ems_isgstat2,tcu1_tqi_tcu_inc as tcu11_tqi_tcu_inc,tcu1_f_tcu as tcu11_f_tcu,tcu1_tcu_type as tcu11_tcu_type,tcu1_tcu_obd as tcu11_tcu_obd,tcu1_swi_gs as tcu11_swi_gs,tcu1_gear_type as tcu11_gear_type,tcu1_tqi_tcu as tcu11_tqi_tcu,tcu1_temp_at as tcu11_temp_at,tcu1_n_tc as tcu11_n_tc,tcu1_swi_cc as tcu11_swi_cc,esp2_lat_accel as esp12_lat_accel,esp2_lat_accel_stat as esp12_lat_accel_stat,esp2_lat_accel_diag as esp12_lat_accel_diag,esp2_long_accel as esp12_long_accel,esp2_long_accel_stat as esp12_long_accel_stat,esp2_long_accel_diag as esp12_long_accel_diag,esp2_cyl_pres as esp12_cyl_pres,esp2_cyl_pres_stat as esp12_cyl_pres_stat,esp2_cyl_press_diag as esp12_cyl_press_diag,esp2_yaw_rate as esp12_yaw_rate,esp2_yaw_rate_stat as esp12_yaw_rate_stat,esp2_yaw_rate_diag as esp12_yaw_rate_diag,sas1_sas_angle as sas11_sas_angle,sas1_sas_speed as sas11_sas_speed,sas1_sas_stat as sas11_sas_stat,tcu2_etl_tcu as tcu12_etl_tcu,tcu2_cur_gr as tcu12_cur_gr,tcu2_vs_tcu as tcu12_vs_tcu,tcu2_fuel_cut_tcu as tcu12_fuel_cut_tcu,tcu2_inh_fuel_cut as tcu12_inh_fuel_cut,tcu2_idle_up_tcu as tcu12_idle_up_tcu,tcu2_n_inc_tcu as tcu12_n_inc_tcu,tcu2_spk_rtd_tcu as tcu12_spk_rtd_tcu,tcu2_n_tc_raw as tcu12_n_tc_raw,ems1_swi_igk as ems11_swi_igk,ems1_f_n_eng as ems11_f_n_eng,ems1_ack_tcs as ems11_ack_tcs,ems1_puc_stat as ems11_puc_stat,ems1_tq_cor_stat as ems11_tq_cor_stat,ems1_rly_ac as ems11_rly_ac,ems1_f_sub_tqi as ems11_f_sub_tqi,ems1_tqi_acor as ems11_tqi_acor,ems1_n as ems11_n,ems1_tqi as ems11_tqi,ems1_tqfr as ems11_tqfr,ems1_vs as ems11_vs,ems4_fco as ems20_fco,ems9_cf_ems_pumptpres as ems20_cf_ems_pumptpres,ems5_ecgpovrd as ems15_ecgpovrd,ems5_qecacc as ems15_qecacc,ems5_ecfail as ems15_ecfail,ems5_fa_pv_can as ems15_fa_pv_can,ems5_intairtemp as ems15_intairtemp,ems5_state_dc_obd as ems15_state_dc_obd,ems5_inh_dc_obd as ems15_inh_dc_obd,ems5_ctr_ig_cyc_obd as ems15_ctr_ig_cyc_obd,ems5_ctr_cdn_obd as ems15_ctr_cdn_obd,ems4_im_autehn as ems14_im_autehn,ems4_l_mil as ems14_l_mil,ems4_amp_can as ems14_amp_can,tcu3_n_tgt_lup as tcu13_n_tgt_lup,tcu3_slope_tcu as tcu13_slope_tcu,tcu3_cf_tcu_inhcda as tcu13_cf_tcu_inhcda,tcu3_cf_tcu_isginhib as tcu13_cf_tcu_isginhib,tcu3_cf_tcu_bkeonreq as tcu13_cf_tcu_bkeonreq,tcu3_cf_tcu_ncstat as tcu13_cf_tcu_ncstat,tcu3_cf_tcu_targr as tcu13_cf_tcu_targr,tcu3_cf_tcu_shfpatt as tcu13_cf_tcu_shfpatt,tcu3_cf_tcu_tqgrdlim as tcu13_cf_tcu_tqgrdlim,ems2_conf_tcu as ems12_conf_tcu,ems2_temp_eng as ems12_temp_eng,ems2_maf_fac_alti_mmv as ems12_maf_fac_alti_mmv,ems2_vb_off_act as ems12_vb_off_act,ems2_ack_es as ems12_ack_es,ems2_od_off_req as ems12_od_off_req,ems2_acc_act as ems12_acc_act,ems2_clu_ack as ems12_clu_ack,ems2_brake_act as ems12_brake_act,ems2_tps as ems12_tps,ems2_pv_av_can as ems12_pv_av_can
  from pt_anal.drv_rg_log_ps_1g
  where sale_vehl_cd = 'DM'
  union all
  select
  vin, sale_vehl_cd, ignitiontime, t, ems19_cf_ems_brkreq,ems19_cf_ems_dnshftreq ,ems19_cf_ems_decelreq ,ems19_cr_ems_bstpre ,ems19_cr_ems_engoiltemp ,ems19_cf_ems_modeledambtemp ,ems19_cf_ems_opsfail ,ems16_tqi_min ,ems16_tqi ,ems16_tqi_target ,ems16_tqi_max ,ems16_cruise_lamp_s ,ems16_pre_fuel_cut_in ,ems16_eng_stat ,ems16_soak_time_error ,ems16_soak_time ,ems16_spk_time_cur ,ems16_cf_ems_aclact ,fatc11_cr_fatc_tqacnout ,fatc11_cf_fatc_acnrqswi ,fatc11_cf_fatc_acncltenrq ,fatc11_cf_fatc_ecvflt ,fatc11_cf_fatc_blwron ,fatc11_cf_fatc_blwrmax ,fatc11_cf_fatc_engstartreq ,fatc11_cf_fatc_isgstopreq ,fatc11_cr_fatc_outtemp ,fatc11_cr_fatc_outtempsns ,fatc11_cf_fatc_compload ,fatc11_cf_fatc_activeeco ,fatc11_cf_fatc_autoactivation ,ems_h12_r_tqacnapvc ,ems_h12_r_pacnc ,ems_h12_tqi_b ,ems_h12_cf_cdastat ,ems_h12_cf_ems_isgstat ,ems_h12_cf_ems_oilchg ,ems_h12_cf_ems_etclimpmod ,ems_h12_r_nengidltgc ,ems_h12_cf_ems_uptargr ,ems_h12_cf_ems_downtargr ,ems_h12_cf_ems_descurgr ,ems_h12_cf_ems_hpresstat ,ems_h12_cf_ems_fcopen ,ems_h12_cf_ems_actecoact ,ems_h12_cf_ems_engrunnorm ,ems_h12_cf_ems_isgstat2 ,tcu11_tqi_tcu_inc ,tcu11_f_tcu ,tcu11_tcu_type ,tcu11_tcu_obd ,tcu11_swi_gs ,tcu11_gear_type ,tcu11_tqi_tcu ,tcu11_temp_at ,tcu11_n_tc ,tcu11_swi_cc ,esp12_lat_accel ,esp12_lat_accel_stat ,esp12_lat_accel_diag ,esp12_long_accel ,esp12_long_accel_stat ,esp12_long_accel_diag ,esp12_cyl_pres ,esp12_cyl_pres_stat ,esp12_cyl_press_diag ,esp12_yaw_rate ,esp12_yaw_rate_stat ,esp12_yaw_rate_diag ,sas11_sas_angle ,sas11_sas_speed ,sas11_sas_stat ,tcu12_etl_tcu ,tcu12_cur_gr ,tcu12_vs_tcu ,tcu12_fuel_cut_tcu ,tcu12_inh_fuel_cut ,tcu12_idle_up_tcu ,tcu12_n_inc_tcu ,tcu12_spk_rtd_tcu ,tcu12_n_tc_raw ,ems11_swi_igk ,ems11_f_n_eng ,ems11_ack_tcs ,ems11_puc_stat ,ems11_tq_cor_stat ,ems11_rly_ac ,ems11_f_sub_tqi ,ems11_tqi_acor ,ems11_n ,ems11_tqi ,ems11_tqfr ,ems11_vs ,ems20_fco ,ems20_cf_ems_pumptpres ,ems15_ecgpovrd ,ems15_qecacc ,ems15_ecfail ,ems15_fa_pv_can ,ems15_intairtemp ,ems15_state_dc_obd ,ems15_inh_dc_obd ,ems15_ctr_ig_cyc_obd ,ems15_ctr_cdn_obd ,ems14_im_autehn ,ems14_l_mil ,ems14_amp_can ,tcu13_n_tgt_lup ,tcu13_slope_tcu ,tcu13_cf_tcu_inhcda ,tcu13_cf_tcu_isginhib ,tcu13_cf_tcu_bkeonreq ,tcu13_cf_tcu_ncstat ,tcu13_cf_tcu_targr ,tcu13_cf_tcu_shfpatt ,tcu13_cf_tcu_tqgrdlim ,ems12_conf_tcu ,ems12_temp_eng ,ems12_maf_fac_alti_mmv ,ems12_vb_off_act ,ems12_ack_es ,ems12_od_off_req ,ems12_acc_act ,ems12_clu_ack ,ems12_brake_act ,ems12_tps ,ems12_pv_av_can
  from pt_anal.drv_rg_log_ps_2g1c
  where sale_vehl_cd = 'TL'
  union all
  select
  vin, sale_vehl_cd, ignitiontime, t, ems19_cf_ems_brkreq,ems19_cf_ems_dnshftreq ,ems19_cf_ems_decelreq ,ems19_cr_ems_bstpre ,ems19_cr_ems_engoiltemp ,ems19_cf_ems_modeledambtemp ,ems19_cf_ems_opsfail ,ems16_tqi_min ,ems16_tqi ,ems16_tqi_target ,ems16_tqi_max ,ems16_cruise_lamp_s ,ems16_pre_fuel_cut_in ,ems16_eng_stat ,ems16_soak_time_error ,ems16_soak_time ,ems16_spk_time_cur ,ems16_cf_ems_aclact ,fatc11_cr_fatc_tqacnout ,fatc11_cf_fatc_acnrqswi ,fatc11_cf_fatc_acncltenrq ,fatc11_cf_fatc_ecvflt ,fatc11_cf_fatc_blwron ,fatc11_cf_fatc_blwrmax ,fatc11_cf_fatc_engstartreq ,fatc11_cf_fatc_isgstopreq ,fatc11_cr_fatc_outtemp ,fatc11_cr_fatc_outtempsns ,fatc11_cf_fatc_compload ,fatc11_cf_fatc_activeeco ,fatc11_cf_fatc_autoactivation ,ems_h12_r_tqacnapvc ,ems_h12_r_pacnc ,ems_h12_tqi_b ,ems_h12_cf_cdastat ,ems_h12_cf_ems_isgstat ,ems_h12_cf_ems_oilchg ,ems_h12_cf_ems_etclimpmod ,ems_h12_r_nengidltgc ,ems_h12_cf_ems_uptargr ,ems_h12_cf_ems_downtargr ,ems_h12_cf_ems_descurgr ,ems_h12_cf_ems_hpresstat ,ems_h12_cf_ems_fcopen ,ems_h12_cf_ems_actecoact ,ems_h12_cf_ems_engrunnorm ,ems_h12_cf_ems_isgstat2 ,tcu11_tqi_tcu_inc ,tcu11_f_tcu ,tcu11_tcu_type ,tcu11_tcu_obd ,tcu11_swi_gs ,tcu11_gear_type ,tcu11_tqi_tcu ,tcu11_temp_at ,tcu11_n_tc ,tcu11_swi_cc ,esp12_lat_accel ,esp12_lat_accel_stat ,esp12_lat_accel_diag ,esp12_long_accel ,esp12_long_accel_stat ,esp12_long_accel_diag ,esp12_cyl_pres ,esp12_cyl_pres_stat ,esp12_cyl_press_diag ,esp12_yaw_rate ,esp12_yaw_rate_stat ,esp12_yaw_rate_diag ,sas11_sas_angle ,sas11_sas_speed ,sas11_sas_stat ,tcu12_etl_tcu ,tcu12_cur_gr ,tcu12_vs_tcu ,tcu12_fuel_cut_tcu ,tcu12_inh_fuel_cut ,tcu12_idle_up_tcu ,tcu12_n_inc_tcu ,tcu12_spk_rtd_tcu ,tcu12_n_tc_raw ,ems11_swi_igk ,ems11_f_n_eng ,ems11_ack_tcs ,ems11_puc_stat ,ems11_tq_cor_stat ,ems11_rly_ac ,ems11_f_sub_tqi ,ems11_tqi_acor ,ems11_n ,ems11_tqi ,ems11_tqfr ,ems11_vs ,ems20_fco ,ems20_cf_ems_pumptpres ,ems15_ecgpovrd ,ems15_qecacc ,ems15_ecfail ,ems15_fa_pv_can ,ems15_intairtemp ,ems15_state_dc_obd ,ems15_inh_dc_obd ,ems15_ctr_ig_cyc_obd ,ems15_ctr_cdn_obd ,ems14_im_autehn ,ems14_l_mil ,ems14_amp_can ,tcu13_n_tgt_lup ,tcu13_slope_tcu ,tcu13_cf_tcu_inhcda ,tcu13_cf_tcu_isginhib ,tcu13_cf_tcu_bkeonreq ,tcu13_cf_tcu_ncstat ,tcu13_cf_tcu_targr ,tcu13_cf_tcu_shfpatt ,tcu13_cf_tcu_tqgrdlim ,ems12_conf_tcu ,ems12_temp_eng ,ems12_maf_fac_alti_mmv ,ems12_vb_off_act ,ems12_ack_es ,ems12_od_off_req ,ems12_acc_act ,ems12_clu_ack ,ems12_brake_act ,ems12_tps ,ems12_pv_av_can
  from pt_anal.drv_rg_log_ps_2g2c
  where sale_vehl_cd = 'UM'
) A
where substr(ignitiontime, 1, 6) > '201406' and substr(ignitiontime, 1, 6) < '201509'
;
-- 1756393506

select * from vcrm_6442267.rg_log_um_dm_tl
where vin = 'KMHJ581ABGU001347' and t='20150522073446'


## 주기적운행로그 중복제거 및 시동직후 2초 제거
drop table if exists vcrm_6442267.rg_log_um_dm_tl_f;
create table vcrm_6442267.rg_log_um_dm_tl_f STORED AS PARQUET
as
select
 A.*
from vcrm_6442267.rg_log_um_dm_tl A
join
(
 select vin, t, count(*) as cnt
 from vcrm_6442267.rg_log_um_dm_tl
 group by vin, t having cnt = 1 -- 중복제거
) B
on A.vin = B.vin and A.t = B.t
where A.t > A.ignitiontime + 1 -- 시동직후 2초 제거
;
-- 1748726469

-- 고속can 1세대 
-- - DM/AG/KH
-- 고속can 2세대 1ch
-- - TL/LQ
-- 고속can 2세대 2ch
-- - UM

## 센서 범위 테이블 생성
-- drop table if exists vcrm_6442267.sensor_range;
-- create table vcrm_6442267.sensor_range(
--   sensor string,
--   min double,
--   max double
-- )
-- ROW FORMAT DELIMITED
-- FIELDS TERMINATED BY '\t'
-- STORED AS TEXTFILE;
-- ;

-- load data inpath '/user/vcrm_6442267/sensors_range.csv' overwrite into table vcrm_6442267.sensor_range


## 범주화(5)
drop table if exists vcrm_6442267.rg_log_um_dm_tl_f5;
create table vcrm_6442267.rg_log_um_dm_tl_f5 STORED AS PARQUET
as
select
 vin
 ,sale_vehl_cd
 ,ignitiontime
 ,t
 ,case when ems16_tqi_min  <= 0 then 0
   when ems16_tqi_min  < 20 then cast(ems16_tqi_min / 4 as int) + 1
   when ems16_tqi_min  < 100 then cast(ems16_tqi_min / 20 as int) + 5
   when ems16_tqi_min  is null then -1
  end as ems16_tqi_min  
 ,case when ems16_tqi  <= 0 then 0
   when ems16_tqi  < 50 then cast(ems16_tqi / 5 as int) + 1
   when ems16_tqi  < 100 then cast(ems16_tqi / 10 as int) + 6
   when ems16_tqi is null then -1
  end as ems16_tqi  
 ,case when ems16_tqi_target  <= 0 then 0
   when ems16_tqi_target  < 50 then cast(ems16_tqi_target / 5 as int) + 1
   when ems16_tqi_target  < 100 then cast(ems16_tqi_target / 10 as int) + 6
   when ems16_tqi_target is null then -1
  end as ems16_tqi_target  
 ,case when ems16_tqi_max  <= 0 then 0
   when ems16_tqi_max  < 20 then cast(ems16_tqi_max / 4 as int) + 1
   when ems16_tqi_max  < 100 then cast(ems16_tqi_max / 20 as int) + 5
   when ems16_tqi_max is null then -1
  end as ems16_tqi_max  

 ,case when ems16_cruise_lamp_s is null then -1
   else cast(ems16_cruise_lamp_s as int) 
  end as ems16_cruise_lamp_s
 ,case when ems16_pre_fuel_cut_in is null then -1 
   else cast(ems16_pre_fuel_cut_in  as int) 
  end as ems16_pre_fuel_cut_in
 ,case when ems16_eng_stat is null then -1
   else cast(ems16_eng_stat as int)
  end as ems16_eng_stat
 ,case when ems16_soak_time_error is null then -1
   else cast(ems16_soak_time_error as int)
  end as ems16_soak_time_error
 ,case when ems16_soak_time  <= 0 then 0
   when ems16_soak_time  <= 10 then 1
   when ems16_soak_time  <= 30 then 2
   when ems16_soak_time  <= 80 then 3
   when ems16_soak_time  <= 130 then 4
   when ems16_soak_time  <= 180 then 5
   when ems16_soak_time  <= 230 then 6
   when ems16_soak_time  <= 255 then 7
   when ems16_soak_time  is null then -1
  end as ems16_soak_time
 ,case when ems16_spk_time_cur  <= -10 then 0
   when ems16_spk_time_cur  <= 0 then 1
   when ems16_spk_time_cur  <= 10 then 2
   when ems16_spk_time_cur  <= 20 then 3
   when ems16_spk_time_cur  <= 40 then 4
   when ems16_spk_time_cur  <= 60 then 5
   when ems16_spk_time_cur  is null then -1
  end as ems16_spk_time_cur
 ,case when ems16_cf_ems_aclact is null then -1
    else cast(ems16_cf_ems_aclact as int)
  end as ems16_cf_ems_aclact

 ,case when fatc11_cr_fatc_tqacnout  <= 0 then 0
   when fatc11_cr_fatc_tqacnout  < 5 then 1
   when fatc11_cr_fatc_tqacnout  < 10 then 2
   when fatc11_cr_fatc_tqacnout  < 20 then 3
   when fatc11_cr_fatc_tqacnout  >= 20 then 4
   when fatc11_cr_fatc_tqacnout  is null then -1
  end as fatc11_cr_fatc_tqacnout
 ,case when fatc11_cf_fatc_acnrqswi is null then -1 else cast(fatc11_cf_fatc_acnrqswi as int) end as fatc11_cf_fatc_acnrqswi 
 ,case when fatc11_cf_fatc_acncltenrq is null then -1 else cast(fatc11_cf_fatc_acncltenrq as int) end as fatc11_cf_fatc_acncltenrq 
 ,case when fatc11_cf_fatc_ecvflt is null then -1 else cast(fatc11_cf_fatc_ecvflt as int) end as fatc11_cf_fatc_ecvflt 
 ,case when fatc11_cf_fatc_blwron is null then -1 else cast(fatc11_cf_fatc_blwron as int) end as fatc11_cf_fatc_blwron 
 ,case when fatc11_cf_fatc_blwrmax is null then -1 else cast(fatc11_cf_fatc_blwrmax as int) end as fatc11_cf_fatc_blwrmax 
 ,case when fatc11_cf_fatc_engstartreq is null then -1 else cast(fatc11_cf_fatc_engstartreq as int) end as fatc11_cf_fatc_engstartreq 
 ,case when fatc11_cf_fatc_isgstopreq is null then -1 else cast(fatc11_cf_fatc_isgstopreq as int) end as fatc11_cf_fatc_isgstopreq 

 ,case when fatc11_cr_fatc_outtemp  <= -10 then 0
   when fatc11_cr_fatc_outtemp  <= 0 then 1
   when fatc11_cr_fatc_outtemp  <= 15 then 2
   when fatc11_cr_fatc_outtemp  <= 25 then 3
   when fatc11_cr_fatc_outtemp  <= 35 then 4
   when fatc11_cr_fatc_outtemp  > 35 then 5
   when fatc11_cr_fatc_outtemp  is null then -1
  end as fatc11_cr_fatc_outtemp
 ,case when fatc11_cr_fatc_outtempsns  <= -40 then 0
   when fatc11_cr_fatc_outtempsns  <= 0 then 1
   when fatc11_cr_fatc_outtempsns  <= 15 then 2
   when fatc11_cr_fatc_outtempsns  <= 25 then 3
   when fatc11_cr_fatc_outtempsns  <= 35 then 4
   when fatc11_cr_fatc_outtempsns  > 35 then 5
   when fatc11_cr_fatc_outtempsns  is null then -1
  end as fatc11_cr_fatc_outtempsns
 ,case when fatc11_cf_fatc_compload is null then -1 else cast(fatc11_cf_fatc_compload as int) end as fatc11_cf_fatc_compload 
 ,case when fatc11_cf_fatc_activeeco is null then -1 else cast(fatc11_cf_fatc_activeeco as int) end as fatc11_cf_fatc_activeeco 
 ,case when fatc11_cf_fatc_autoactivation is null then -1 else cast(fatc11_cf_fatc_autoactivation as int) end as fatc11_cf_fatc_autoactivation 
 
 ,case when ems_h12_r_tqacnapvc  <= 0 then 0
   when ems_h12_r_tqacnapvc  < 51 then cast(ems_h12_r_tqacnapvc / 10 as int) + 1
   when ems_h12_r_tqacnapvc is null then -1
  end as ems_h12_r_tqacnapvc
 ,case when ems_h12_r_pacnc  <= 0 then 0
   when ems_h12_r_pacnc  <= 1000 then 1
   when ems_h12_r_pacnc  <= 4000 then 2
   when ems_h12_r_pacnc  <= 7000 then 3
   when ems_h12_r_pacnc  <= 10000 then 4
   when ems_h12_r_pacnc  <= 13000 then 5 
   when ems_h12_r_pacnc  <= 15000 then 6
   when ems_h12_r_pacnc  <= 20000 then 7
   when ems_h12_r_pacnc  <= 25000 then 8
   when ems_h12_r_pacnc  <= 30000 then 9
   when ems_h12_r_pacnc  > 30000 then 10
   when ems_h12_r_pacnc  is null then -1
  end as ems_h12_r_pacnc
 ,case when ems_h12_tqi_b  <= 0 then 0
   when ems_h12_tqi_b  < 50 then cast(ems_h12_tqi_b / 5 as int) + 1
   when ems_h12_tqi_b  < 100 then cast(ems_h12_tqi_b / 10 as int) + 6
   when ems_h12_tqi_b is null then -1
  end as ems_h12_tqi_b 

 ,case when ems_h12_cf_cdastat is null then -1 else cast(ems_h12_cf_cdastat as int) end as ems_h12_cf_cdastat 
 ,case when ems_h12_cf_ems_isgstat is null then -1 else cast(ems_h12_cf_ems_isgstat as int) end as ems_h12_cf_ems_isgstat 
 ,case when ems_h12_cf_ems_oilchg is null then -1 else cast(ems_h12_cf_ems_oilchg as int) end as ems_h12_cf_ems_oilchg 
 ,case when ems_h12_cf_ems_etclimpmod is null then -1 else cast(ems_h12_cf_ems_etclimpmod as int) end as ems_h12_cf_ems_etclimpmod 
 
 ,case when ems_h12_r_nengidltgc  <= 0 then 0
   when ems_h12_r_nengidltgc  <= 550 then 1
   when ems_h12_r_nengidltgc  <= 780 then 2
   when ems_h12_r_nengidltgc  <= 800 then 3
   when ems_h12_r_nengidltgc  <= 820 then 4
   when ems_h12_r_nengidltgc  <= 840 then 5
   when ems_h12_r_nengidltgc  <= 860 then 6
   when ems_h12_r_nengidltgc  <= 880 then 7
   when ems_h12_r_nengidltgc  <= 900 then 8
   when ems_h12_r_nengidltgc  <= 1000 then 9
   when ems_h12_r_nengidltgc  > 1000 then 10
   when ems_h12_r_nengidltgc  is null then -1
  end as ems_h12_r_nengidltgc
 ,case when ems_h12_cf_ems_uptargr  is null then -1 else cast(ems_h12_cf_ems_uptargr as int) end as ems_h12_cf_ems_uptargr 
 ,case when ems_h12_cf_ems_downtargr  is null then -1 else cast(ems_h12_cf_ems_downtargr as int) end as ems_h12_cf_ems_downtargr 
 ,case when ems_h12_cf_ems_descurgr  is null then -1 else cast(ems_h12_cf_ems_descurgr as int) end as ems_h12_cf_ems_descurgr
 ,case when ems_h12_cf_ems_hpresstat  is null then -1 else cast(ems_h12_cf_ems_hpresstat as int) end as ems_h12_cf_ems_hpresstat 
 ,case when ems_h12_cf_ems_fcopen  is null then -1 else cast(ems_h12_cf_ems_fcopen as int) end as ems_h12_cf_ems_fcopen 
 ,case when ems_h12_cf_ems_actecoact  is null then -1 else cast(ems_h12_cf_ems_actecoact as int) end as ems_h12_cf_ems_actecoact 
 ,case when ems_h12_cf_ems_engrunnorm  is null then -1 else cast(ems_h12_cf_ems_engrunnorm as int) end as ems_h12_cf_ems_engrunnorm 
 ,case when ems_h12_cf_ems_isgstat2  is null then -1 else cast(ems_h12_cf_ems_isgstat2 as int) end as ems_h12_cf_ems_isgstat2 
 
 ,case when tcu11_tqi_tcu_inc  <= 0 then 0
   when tcu11_tqi_tcu_inc  < 15 then cast(tcu11_tqi_tcu_inc / 3 as int) + 1
   when tcu11_tqi_tcu_inc  >= 15 then 6
   when tcu11_tqi_tcu_inc  is null then -1
  end as tcu11_tqi_tcu_inc  

 ,case when tcu11_f_tcu is null then -1 else cast(tcu11_f_tcu as int) end as tcu11_f_tcu 
 ,case when tcu11_tcu_type is null then -1 else cast(tcu11_tcu_type as int) end as tcu11_tcu_type 
 ,case when tcu11_tcu_obd is null then -1 else cast(tcu11_tcu_obd as int) end as tcu11_tcu_obd 
 ,case when tcu11_swi_gs is null then -1 else cast(tcu11_swi_gs as int) end as tcu11_swi_gs 
 ,case when tcu11_gear_type is null then -1 else cast(tcu11_gear_type as int) end as tcu11_gear_type 
 ,case when tcu11_tqi_tcu  <= 0 then 0
   when tcu11_tqi_tcu  < 60 then cast(tcu11_tqi_tcu / 10 as int) + 1
   when tcu11_tqi_tcu  >= 60 then 7
   when tcu11_tqi_tcu  is null then -1
  end as tcu11_tqi_tcu 
 ,case when tcu11_temp_at <= 0 then 0
  when tcu11_temp_at < 100 then cast(tcu11_temp_at / 10 as int) + 1
  when tcu11_temp_at >= 100 then 11
  when tcu11_temp_at is null then -1
  end as tcu11_temp_at
 ,case when tcu11_n_tc <= 0 then 0
  when tcu11_n_tc < 3000 then cast(tcu11_n_tc / 500 as int) + 1
  when tcu11_n_tc >= 3000 then 7
  when tcu11_n_tc is null then -1
  end as tcu11_n_tc
 ,case when tcu11_swi_cc is null then -1 else cast(tcu11_swi_cc as int) end as tcu11_swi_cc 
 ,case when esp12_lat_accel <= -3 then 0
    when esp12_lat_accel <= -1 then 1
    when esp12_lat_accel < 0 then 2
    when esp12_lat_accel = 0 then 3
    when esp12_lat_accel < 1 then 4
    when esp12_lat_accel < 3 then 5
    when esp12_lat_accel >= 3 then 6
    when esp12_lat_accel is null then -1
  end as esp12_lat_accel
 ,case when esp12_lat_accel_stat is null then -1 else cast(esp12_lat_accel_stat as int) end as esp12_lat_accel_stat 
 ,case when esp12_lat_accel_diag is null then -1 else cast(esp12_lat_accel_diag as int) end as esp12_lat_accel_diag 
 ,case when esp12_long_accel <= -6 then 0
    when esp12_long_accel <= -3 then 1
    when esp12_long_accel < -1 then 2
    when esp12_long_accel < 0 then 3
    when esp12_long_accel = 0 then 4
    when esp12_long_accel < 1 then 5
    when esp12_long_accel < 3 then 6
    when esp12_long_accel < 6 then 7
    when esp12_long_accel >= 6 then 8
    when esp12_long_accel is null then -1
  end as esp12_long_accel
 ,case when esp12_long_accel_stat is null then -1 else cast(esp12_long_accel_stat as int) end as esp12_long_accel_stat 
 ,case when esp12_long_accel_diag is null then -1 else cast(esp12_long_accel_diag as int) end as esp12_long_accel_diag 
 ,case when esp12_cyl_pres <= 0 then 0
    when esp12_cyl_pres < 20 then cast(esp12_cyl_pres / 5 as int) + 1
    when esp12_cyl_pres < 70 then cast(esp12_cyl_pres / 10 as int) + 3
    when esp12_cyl_pres >= 70 then 3
    when esp12_cyl_pres is null then -1
  end as esp12_cyl_pres
 ,case when esp12_cyl_pres_stat  is null then -1 else cast(esp12_cyl_pres_stat as int) end as esp12_cyl_pres_stat 
 ,case when esp12_cyl_press_diag is null then -1 else cast(esp12_cyl_press_diag as int)end as esp12_cyl_press_diag 
 ,case when esp12_yaw_rate <= -6 then 0
    when esp12_yaw_rate <= -3 then 1
    when esp12_yaw_rate < 0 then 2
    when esp12_yaw_rate = 0 then 3
    when esp12_yaw_rate < 3 then 4
    when esp12_yaw_rate < 6 then 5
    when esp12_yaw_rate >= 6 then 6
    when esp12_yaw_rate is null then -1
  end as esp12_yaw_rate
 ,case when esp12_yaw_rate_stat is null then -1 else cast(esp12_yaw_rate_stat as int) end as esp12_yaw_rate_stat 
 ,case when esp12_yaw_rate_diag is null then -1 else cast(esp12_yaw_rate_diag as int) end as esp12_yaw_rate_diag 

 ,case when sas11_sas_angle <= -3270 then 0
    when sas11_sas_angle > -3270 and sas11_sas_angle < -250 then 1
    when sas11_sas_angle >= -250 and sas11_sas_angle < 250 then 2
    when sas11_sas_angle >= 250 and sas11_sas_angle < 3270 then 3
    when sas11_sas_angle >= 3270 then 4
    when sas11_sas_angle is null then -1
  end as sas11_sas_angle
 ,case when sas11_sas_speed <= 0 then 0
    when sas11_sas_speed > 0 and sas11_sas_speed < 300 then 1
    when sas11_sas_speed >= 300 and sas11_sas_speed < 600 then 2
    when sas11_sas_speed >= 600 and sas11_sas_speed < 1016 then 3
    when sas11_sas_speed >= 1016 then 4
    when sas11_sas_speed is null then -1
  end as sas11_sas_speed
 ,case when sas11_sas_stat is null then -1 else cast(sas11_sas_stat as int) end as sas11_sas_stat
 
 ,case when tcu12_etl_tcu <= 0 then 0
    when tcu12_etl_tcu < 400 then cast(tcu12_etl_tcu / 50 as int) + 1
    when tcu12_etl_tcu >= 400 then 9
    when tcu12_etl_tcu is null then -1
  end as tcu12_etl_tcu
 ,case when tcu12_cur_gr is null then -1 else cast(tcu12_cur_gr as int) end as tcu12_cur_gr
 ,case when tcu12_vs_tcu <= 0 then 0
    when tcu12_vs_tcu < 160 then cast(tcu12_vs_tcu / 20 as int) + 1
    when tcu12_vs_tcu >= 160 then 9
    when tcu12_vs_tcu is null then -1
  end as tcu12_vs_tcu
 ,case when tcu12_fuel_cut_tcu is null then -1 else cast(tcu12_fuel_cut_tcu as int) end as tcu12_fuel_cut_tcu 
 ,case when tcu12_inh_fuel_cut is null then -1 else cast(tcu12_inh_fuel_cut as int) end as tcu12_inh_fuel_cut 
 ,case when tcu12_idle_up_tcu is null then -1 else cast(tcu12_idle_up_tcu as int) end as tcu12_idle_up_tcu 
 ,case when tcu12_n_inc_tcu is null then -1 else cast(tcu12_n_inc_tcu as int) end as tcu12_n_inc_tcu 
 ,case when tcu12_spk_rtd_tcu <= -15 then 0
    when tcu12_spk_rtd_tcu < 0 then 1
    when tcu12_spk_rtd_tcu < 15 then 2
    when tcu12_spk_rtd_tcu >= 15 then 3
    when tcu12_spk_rtd_tcu is null then -1
  end as tcu12_spk_rtd_tcu
 ,case when tcu12_n_tc_raw <= 0 then 0
    when tcu12_n_tc_raw < 2400 then cast(tcu12_n_tc_raw / 300 as int) + 1
    when tcu12_n_tc_raw >= 2400 then 9
    when tcu12_n_tc_raw is null then -1
  end as tcu12_n_tc_raw

 ,case when ems11_swi_igk is null then -1 else cast(ems11_swi_igk as int) end as ems11_swi_igk 
 ,case when ems11_f_n_eng is null then -1 else cast(ems11_f_n_eng as int) end as ems11_f_n_eng 
 ,case when ems11_ack_tcs is null then -1 else cast(ems11_ack_tcs as int) end as ems11_ack_tcs 
 ,case when ems11_puc_stat is null then -1 else cast(ems11_puc_stat as int) end as ems11_puc_stat 
 ,case when ems11_tq_cor_stat is null then -1 else cast(ems11_tq_cor_stat as int) end as ems11_tq_cor_stat 
 ,case when ems11_rly_ac is null then -1 else cast(ems11_rly_ac as int) end as ems11_rly_ac 
 ,case when ems11_f_sub_tqi is null then -1 else cast(ems11_f_sub_tqi as int) end as ems11_f_sub_tqi 
 ,case when ems11_tqi_acor <= 0 then 0
    when ems11_tqi_acor  < 50 then cast(ems11_tqi_acor / 5 as int) + 1
    when ems11_tqi_acor  < 100 then cast(ems11_tqi_acor / 10 as int) + 6
    when ems11_tqi_acor is null then -1
  end as ems11_tqi_acor
 ,case when ems11_n <= 0 then 0
    when ems11_n < 530 then 1
    when ems11_n < 730 then 2
    when ems11_n < 1000 then 3
    when ems11_n < 1500 then 4
    when ems11_n < 2000 then 5
    when ems11_n <= 7000 then cast(ems11_n / 1000 as int) + 4
    when ems11_n is null then -1
  end as ems11_n
 ,case when ems11_tqi <= 0 then 0
   when ems11_tqi  < 50 then cast(ems11_tqi / 5 as int) + 1
   when ems11_tqi  < 100 then cast(ems11_tqi / 10 as int) + 6
   when ems11_tqi is null then -1
  end as ems11_tqi
 ,case when ems11_tqfr <= 0 then 0
   when ems11_tqfr  < 50 then cast(ems11_tqfr / 5 as int) + 1
   when ems11_tqfr  < 100 then cast(ems11_tqfr / 10 as int) + 6
   when ems11_tqfr is null then -1
  end as ems11_tqfr
 ,case when ems11_vs <= 0 then 0
   when ems11_vs  < 60 then cast(ems11_vs / 10 as int) + 1
   when ems11_vs  < 120 then cast(ems11_vs / 20 as int) + 4
   when ems11_vs  >= 120 then 10
   when ems11_vs is null then -1
  end as ems11_vs

 ,case when ems20_fco <= 0 then 0
    when ems20_fco < 40 then cast(ems20_fco / 5 as int ) + 1
    when ems20_fco < 70 then 9
    when ems20_fco < 100 then 10
    when ems20_fco < 130 then 11
    when ems20_fco < 160 then 12
    when ems20_fco >= 160 then 13
    when ems20_fco is null then -1
  end as ems20_fco
 
 ,case when ems15_ecgpovrd is null then -1 else cast(ems15_ecgpovrd as int) end as ems15_ecgpovrd 
 ,case when ems15_qecacc is null then -1 else cast(ems15_qecacc as int) end as ems15_qecacc 
 ,case when ems15_ecfail is null then -1 else cast(ems15_ecfail as int) end as ems15_ecfail 
 ,case when ems15_fa_pv_can is null then -1 else cast(ems15_fa_pv_can as int) end as ems15_fa_pv_can
 ,case when ems15_intairtemp <= 0 then 0
    when ems15_intairtemp < 100 then cast(ems15_intairtemp / 20 as int) + 1
    when ems15_intairtemp >= 100 then 6
    when ems15_intairtemp is null then -1
  end as ems15_intairtemp
 ,case when ems15_state_dc_obd is null then -1 else cast(ems15_state_dc_obd as int) end as ems15_state_dc_obd
 ,case when ems15_inh_dc_obd is null then -1 else cast(ems15_inh_dc_obd  as int) end as ems15_inh_dc_obd 
 ,case when ems15_ctr_ig_cyc_obd <= 0 then 0
    when ems15_ctr_ig_cyc_obd is null then -1
    else cast(ems15_ctr_ig_cyc_obd / 50 as int) + 1
  end as ems15_ctr_ig_cyc_obd
 ,case when ems15_ctr_cdn_obd <= 0 then 0
    when ems15_ctr_cdn_obd <= 100 then cast(ems15_ctr_cdn_obd / 20 as int) + 1
    when ems15_ctr_cdn_obd is null then -1
  end as ems15_ctr_cdn_obd
 ,case when ems14_im_autehn is null then -1 else cast(ems14_im_autehn as int) end as ems14_im_autehn 
 ,case when ems14_l_mil is null then -1 else cast(ems14_l_mil as int) end as ems14_l_mil 
 
 ,case when ems14_amp_can <= 500 then 0
    when ems14_amp_can < 600 then 1
    when ems14_amp_can < 700 then 2
    when ems14_amp_can < 730 then 3
    when ems14_amp_can < 760 then 4
    when ems14_amp_can >= 760 then 5
    when ems14_amp_can is null then -1
  end as ems14_amp_can
 
 ,case when tcu13_n_tgt_lup <= 500 then 0
    when tcu13_n_tgt_lup < 3040 then cast(tcu13_n_tgt_lup / 500 as int)
    when tcu13_n_tgt_lup >= 3040 then 7
    when tcu13_n_tgt_lup is null then -1
  end as tcu13_n_tgt_lup

 ,case when tcu13_slope_tcu < 0 then cast(tcu13_slope_tcu / 5 as int)
    when tcu13_slope_tcu = 0 then 1
    when tcu13_slope_tcu < 16 then cast(tcu13_slope_tcu / 5 as int) + 2
    when tcu13_slope_tcu is null then -1
  end as tcu13_slope_tcu
 ,case when tcu13_cf_tcu_inhcda is null then -1 else cast(tcu13_cf_tcu_inhcda as int) end as tcu13_cf_tcu_inhcda 
 ,case when tcu13_cf_tcu_isginhib is null then -1 else cast(tcu13_cf_tcu_isginhib as int) end as tcu13_cf_tcu_isginhib 
 ,case when tcu13_cf_tcu_bkeonreq is null then -1 else cast(tcu13_cf_tcu_bkeonreq as int) end as tcu13_cf_tcu_bkeonreq 
 ,case when tcu13_cf_tcu_ncstat  is null then -1 else cast(tcu13_cf_tcu_ncstat as int) end as tcu13_cf_tcu_ncstat 
 ,case when tcu13_cf_tcu_targr is null then -1 else cast(tcu13_cf_tcu_targr as int) end as tcu13_cf_tcu_targr
 ,case when tcu13_cf_tcu_shfpatt is null then -1 else cast(tcu13_cf_tcu_shfpatt as int) end as tcu13_cf_tcu_shfpatt
 
 ,case when tcu13_cf_tcu_tqgrdlim <= 0 then 0
    when tcu13_cf_tcu_tqgrdlim < 2540 then cast(tcu13_cf_tcu_tqgrdlim / 500 as int) + 1
    when tcu13_cf_tcu_tqgrdlim >= 2540 then 7
    when tcu13_cf_tcu_tqgrdlim is null then -1
  end as tcu13_cf_tcu_tqgrdlim
 
 ,case when ems12_conf_tcu <= 0 then 0
    when ems12_conf_tcu > 0 and ems12_conf_tcu < 20 then 1
    when ems12_conf_tcu >= 20 and ems12_conf_tcu < 40 then 2
    when ems12_conf_tcu >= 40 and ems12_conf_tcu < 60 then 3
    when ems12_conf_tcu >= 60 then 4
    when ems12_conf_tcu is null then -1
  end as ems12_conf_tcu
 
 ,case when ems12_temp_eng < 80 then cast(ems12_temp_eng / 20 as int)
    when ems12_temp_eng < 95 then 4
    when ems12_temp_eng < 100 then 5
    when ems12_temp_eng < 105 then 6
    when ems12_temp_eng >= 105 then 7
    when ems12_temp_eng is null then -1
  end as ems12_temp_eng
 ,case when ems12_maf_fac_alti_mmv <= 2 then cast(ems12_maf_fac_alti_mmv / 0.4 as int)
    when ems12_maf_fac_alti_mmv is null then -1
  end as ems12_maf_fac_alti_mmv
 ,case when ems12_vb_off_act  is null then -1 else cast(ems12_vb_off_act as int) end as ems12_vb_off_act 
 ,case when ems12_ack_es  is null then -1 else cast(ems12_ack_es as int) end as ems12_ack_es 
 ,case when ems12_od_off_req  is null then -1 else cast(ems12_od_off_req as int) end as ems12_od_off_req 
 ,case when ems12_acc_act  is null then -1 else cast(ems12_acc_act as int) end as ems12_acc_act 
 ,case when ems12_clu_ack  is null then -1 else cast(ems12_clu_ack as int) end as ems12_clu_ack 
 ,case when ems12_brake_act  is null then -1 else cast(ems12_brake_act as int) end as ems12_brake_act 

 ,case when ems12_tps <= 0 then 0
    when ems12_tps  < 50 then cast(ems12_tps / 5 as int) + 1
    when ems12_tps  < 100 then cast(ems12_tps / 10 as int) + 6
    when ems12_tps  >= 100 then 16
    when ems12_tps is null then -1
  end as ems12_tps
 ,case when ems12_pv_av_can <= 0 then 0
    when ems12_pv_av_can  < 50 then cast(ems12_pv_av_can / 5 as int) + 1
    when ems12_pv_av_can  < 100 then cast(ems12_pv_av_can / 10 as int) + 6
    when ems12_pv_av_can  >= 100 then 16
    when ems12_pv_av_can is null then -1
  end as ems12_pv_av_can
 
 from vcrm_6442267.rg_log_um_dm_tl_f
 ;
-- 1748726469

## 범주화 데이터 + 고도

-- 상시테이블 
-- select min(ignitiontime), max(ignitiontime)
-- from gps_log_st_road_alt_weather
-- where substr(ignitiontime,1,8) > '20100101' and substr(ignitiontime,1,8) < '20160101'
-- min : 2014-06-06 16:14:29.0, max : 2015-08-04 22:47:54.0

-- drop table if exists vcrm_6442267.rg_log_um_dm_tl_f5_;
-- create table vcrm_6442267.rg_log_um_dm_tl_f5_
-- as
-- select 
--  A.*
--  , case when B.road_slope < -60 then 0 
--      when B.road_slope < -50 then 1
--      when B.road_slope < -40 then 2
--      when B.road_slope < -30 then 3
--      when B.road_slope < -20 then 4
--      when B.road_slope < -10 then 5
--      when B.road_slope < 0 then 6
--      when B.road_slope = 0 then 7
--      when B.road_slope < 10 then 8
--      when B.road_slope < 20 then 9
--      when B.road_slope < 30 then 10
--      when B.road_slope < 40 then 11
--      when B.road_slope < 50 then 12
--      when B.road_slope < 60 then 13
--      when B.road_slope >= 60 then 14
--    end as road_slope     
-- from vcrm_6442267.rg_log_um_dm_tl_f5 A
-- join pt_anal.rt_log_mstr B
-- on A.vin = B.vin and from_unixtime(unix_timestamp(A.t, 'yyyyMMddHHmmss')) = B.t
-- ;
-- 961178474

-- 30초 단위로 1초단위 매핑 불가. 고도가 급격하게 변하는 현상 발생.

## 범주화 운행로그 + ID
drop table if exists vcrm_6442267.rg_log_um_dm_tl_f5_id;
create table vcrm_6442267.rg_log_um_dm_tl_f5_id STORED AS PARQUET
as
select
 *
 ,concat(ems16_tqi_min,ems16_tqi,ems16_tqi_target,ems16_tqi_max,ems16_cruise_lamp_s,ems16_pre_fuel_cut_in,ems16_eng_stat,ems16_soak_time_error,ems16_soak_time,ems16_spk_time_cur,ems16_cf_ems_aclact,fatc11_cr_fatc_tqacnout,fatc11_cf_fatc_acnrqswi,fatc11_cf_fatc_acncltenrq,fatc11_cf_fatc_ecvflt,fatc11_cf_fatc_blwron,fatc11_cf_fatc_blwrmax,fatc11_cf_fatc_engstartreq,fatc11_cf_fatc_isgstopreq,fatc11_cr_fatc_outtemp,fatc11_cr_fatc_outtempsns,fatc11_cf_fatc_compload,fatc11_cf_fatc_activeeco,fatc11_cf_fatc_autoactivation,ems_h12_r_tqacnapvc,ems_h12_r_pacnc,ems_h12_tqi_b,ems_h12_cf_cdastat,ems_h12_cf_ems_isgstat,ems_h12_cf_ems_oilchg,ems_h12_cf_ems_etclimpmod,ems_h12_r_nengidltgc,ems_h12_cf_ems_uptargr,ems_h12_cf_ems_downtargr,ems_h12_cf_ems_descurgr,ems_h12_cf_ems_hpresstat,ems_h12_cf_ems_fcopen,ems_h12_cf_ems_actecoact,ems_h12_cf_ems_engrunnorm,ems_h12_cf_ems_isgstat2,tcu11_tqi_tcu_inc,tcu11_f_tcu,tcu11_tcu_type,tcu11_tcu_obd,tcu11_swi_gs,tcu11_gear_type,tcu11_tqi_tcu,tcu11_temp_at,tcu11_n_tc,tcu11_swi_cc,esp12_lat_accel,esp12_lat_accel_stat,esp12_lat_accel_diag,esp12_long_accel,esp12_long_accel_stat,esp12_long_accel_diag,esp12_cyl_pres,esp12_cyl_pres_stat,esp12_cyl_press_diag,esp12_yaw_rate,esp12_yaw_rate_stat,esp12_yaw_rate_diag,sas11_sas_angle,sas11_sas_speed,sas11_sas_stat,tcu12_etl_tcu,tcu12_cur_gr,tcu12_vs_tcu,tcu12_fuel_cut_tcu,tcu12_inh_fuel_cut,tcu12_idle_up_tcu,tcu12_n_inc_tcu,tcu12_spk_rtd_tcu,tcu12_n_tc_raw,ems11_swi_igk,ems11_f_n_eng,ems11_ack_tcs,ems11_puc_stat,ems11_tq_cor_stat,ems11_rly_ac,ems11_f_sub_tqi,ems11_tqi_acor,ems11_n,ems11_tqi,ems11_tqfr,ems11_vs,ems20_fco,ems15_ecgpovrd,ems15_qecacc,ems15_ecfail,ems15_fa_pv_can,ems15_intairtemp,ems15_state_dc_obd,ems15_inh_dc_obd,ems15_ctr_ig_cyc_obd,ems15_ctr_cdn_obd,ems14_im_autehn,ems14_l_mil,ems14_amp_can,tcu13_n_tgt_lup,tcu13_slope_tcu,tcu13_cf_tcu_inhcda,tcu13_cf_tcu_isginhib,tcu13_cf_tcu_bkeonreq,tcu13_cf_tcu_ncstat,tcu13_cf_tcu_targr,tcu13_cf_tcu_shfpatt,tcu13_cf_tcu_tqgrdlim,ems12_conf_tcu,ems12_temp_eng,ems12_maf_fac_alti_mmv,ems12_vb_off_act,ems12_ack_es,ems12_od_off_req,ems12_acc_act,ems12_clu_ack,ems12_brake_act,ems12_tps,ems12_pv_av_can) as id
from vcrm_6442267.rg_log_um_dm_tl_f5
;


## 신규 센서패턴 생성
drop table if exists vcrm_6442267.rg_log_um_dm_tl_pattern;
create table vcrm_6442267.rg_log_um_dm_tl_pattern STORED AS PARQUET
as
select
 id
,count(*) as cnt
from vcrm_6442267.rg_log_um_dm_tl_f5_id
--where substr(ignitiontime,1,6) like '201411%'
group by id
;


### 이력 테이블 구현
--- id/created_dt/updated_dt/cnt/pct/total_cnt
drop table if exists vcrm_6442267.rg_log_um_dm_tl_history;
create table vcrm_6442267.rg_log_um_dm_tl_history(
  id string,
  created_dt string,
  updated_dt string,
  new_cnt bigint,
  cnt bigint,
  pct double,
  total_cnt bigint
) stored as PARQUET
;


--- 1) 이미 있는 패턴의 경우, 
--- id/created_dt 기존값 유지, updated_dt/cnt/pct/rank/total_cnt 신규 생성
insert into table vcrm_6442267.rg_log_um_dm_tl_history
select
 A.id
 , A.created_dt
 , from_unixtime(unix_timestamp(), 'yyyyMMdd') as updated_dt
 , B.cnt as new_cnt
 , (A.cnt + B.cnt) as cnt
 , (A.cnt + B.cnt) / (A.total_cnt + C.new_total_cnt) as pct
 , (A.total_cnt + C.new_total_cnt) as total_cnt
from 
(
  select 
    C.*
  from vcrm_6442267.rg_log_um_dm_tl_history C
   join 
   (
    select id, max(updated_dt) as updated_dt
    from vcrm_6442267.rg_log_um_dm_tl_history 
    group by id
   ) D
   on C.id = D.id and C.updated_dt = D.updated_dt
) A 
join vcrm_6442267.rg_log_um_dm_tl_pattern B
 on A.id = B.id
cross join (select sum(cnt) as new_total_cnt from vcrm_6442267.rg_log_um_dm_tl_pattern) C
;


--- 2) 신규 패턴의 경우
--- id/created_dt/updated_dt/cnt/pct/rank/total_cnt 신규 생성
insert into table vcrm_6442267.rg_log_um_dm_tl_history
select
 A.id
 , from_unixtime(unix_timestamp(), 'yyyyMMdd') as created_dt
 , from_unixtime(unix_timestamp(), 'yyyyMMdd') as updated_dt
 , A.cnt as new_cnt
 , A.cnt
 , case when B.total_cnt is not null then A.cnt / (B.total_cnt + C.new_total_cnt) 
        else A.cnt / C.new_total_cnt -- history 테이블이 비어있는 경우
   end as pct
 , case when B.total_cnt is not null then B.total_cnt + C.new_total_cnt 
        else C.new_total_cnt -- history 테이블이 비어있는 경우
   end as total_cnt
from 
(
  select 
    *
  from vcrm_6442267.rg_log_um_dm_tl_pattern C
  where C.id not in (select id from vcrm_6442267.rg_log_um_dm_tl_history group by id)
) A
cross join (select max(total_cnt) as total_cnt from vcrm_6442267.rg_log_um_dm_tl_history where updated_dt < ${p_date}) B
cross join (select sum(cnt) as new_total_cnt from vcrm_6442267.rg_log_um_dm_tl_pattern) C
;
-- 1072708296


## 최신 센서패턴 생성
drop table if exists vcrm_6442267.rg_log_um_dm_tl_latest;
create table vcrm_6442267.rg_log_um_dm_tl_latest STORED AS PARQUET
as
select 
    C.id, C.cnt, C.pct, C.total_cnt
  from vcrm_6442267.rg_log_um_dm_tl_history C
   join 
   (
    select id, max(updated_dt) as updated_dt
    from vcrm_6442267.rg_log_um_dm_tl_history 
    group by id
   ) D
   on C.id = D.id and C.updated_dt = D.updated_dt
;


## 운행로그 + cnt + pct 
## 최신 또는 특정시점의 패턴을 붙이는 형태
drop table if exists vcrm_6442267.rg_log_um_dm_tl_f5_id_cnt_pct;
create table vcrm_6442267.rg_log_um_dm_tl_f5_id_cnt_pct STORED AS PARQUET
as
select
  A.*
  , B.cnt as cnt -- 조합 빈도수
  , B.pct as pct  -- 조합 발생확률
from 
  vcrm_6442267.rg_log_um_dm_tl_f5_id A
left outer join 
  vcrm_6442267.rg_log_um_dm_tl_latest B
on A.id = B.id
;  

---> 위 테이블을 이용해 다시 목적별 특정 센서군만 이용해 패턴을 가공할 수 있음.

------------------------------------------------------------------------------

