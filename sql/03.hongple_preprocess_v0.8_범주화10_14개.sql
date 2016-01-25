## 이상패턴탐지 분석
## 범주화(10) 가공 및 DTC 매칭율
## created by Joel Lee
## since 2015.9.23
## 대상 데이터 : 주기적운행로그(2014.09.28~2015.08.02, DM/UM/TL)

## 범주화(10)
drop table if exists vcrm_6442267.rg_log_um_dm_tl_f10;
create table vcrm_6442267.rg_log_um_dm_tl_f10 STORED AS PARQUET
as
select
 vin
 ,sale_vehl_cd
 ,ignitiontime
 ,t
 ,case when ems16_tqi_min  <= 0 then 0
   when ems16_tqi_min  < 100 then cast(ems16_tqi_min / 10 as int)
   when ems16_tqi_min  is null then -1
  end as ems16_tqi_min  
 ,case when ems16_tqi  <= 0 then 0
   when ems16_tqi  < 100 then cast(ems16_tqi / 10 as int)
   when ems16_tqi is null then -1
  end as ems16_tqi  
 ,case when ems16_tqi_target  <= 0 then 0
   when ems16_tqi_target  < 100 then cast(ems16_tqi_target / 10 as int)
   when ems16_tqi_target is null then -1
  end as ems16_tqi_target  
 ,case when ems16_tqi_max  <= 0 then 0
   when ems16_tqi_max  < 100 then cast(ems16_tqi_max / 10 as int)
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
   when ems16_spk_time_cur  <= 30 then 4
   when ems16_spk_time_cur  <= 40 then 5
   when ems16_spk_time_cur  <= 50 then 6
   when ems16_spk_time_cur  <= 60 then 7
   when ems16_spk_time_cur  <= 70 then 8
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
   when ems_h12_r_pacnc  <= 15000 then 5
   when ems_h12_r_pacnc  <= 20000 then 6
   when ems_h12_r_pacnc  <= 25000 then 7
   when ems_h12_r_pacnc  <= 30000 then 8
   when ems_h12_r_pacnc  > 30000 then 9
   when ems_h12_r_pacnc  is null then -1
  end as ems_h12_r_pacnc
 ,case when ems_h12_tqi_b  <= 0 then 0
   when ems_h12_tqi_b  < 100 then cast(ems_h12_tqi_b / 10 as int)
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
   when ems_h12_r_nengidltgc  <= 900 then 7
   when ems_h12_r_nengidltgc  <= 1000 then 8
   when ems_h12_r_nengidltgc  > 1000 then 9
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
  when tcu11_temp_at < 100 then cast(tcu11_temp_at / 15 as int) + 1
  when tcu11_temp_at >= 100 then 8
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
    when esp12_cyl_pres < 70 then cast(esp12_cyl_pres / 10 as int) + 1
    when esp12_cyl_pres >= 70 then 8
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
 ,case when tcu12_cur_gr is null then -1 
    when tcu12_cur_gr = 14 then 9
    else cast(tcu12_cur_gr as int) 
  end as tcu12_cur_gr
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
    when ems11_tqi_acor  < 100 then cast(ems11_tqi_acor / 10 as int)
    when ems11_tqi_acor is null then -1
  end as ems11_tqi_acor
 ,case when ems11_n <= 0 then 0
    when ems11_n < 530 then 1
    when ems11_n < 730 then 2
    when ems11_n < 1000 then 3
    when ems11_n < 1500 then 4
    when ems11_n < 2000 then 5
    when ems11_n < 3000 then 6
    when ems11_n < 4500 then 7
    when ems11_n < 6000 then 8
    when ems11_n <= 7000 then 9
    when ems11_n is null then -1
  end as ems11_n
 ,case when ems11_tqi <= 0 then 0
   when ems11_tqi  < 100 then cast(ems11_tqi / 10 as int)
   when ems11_tqi is null then -1
  end as ems11_tqi
 ,case when ems11_tqfr <= 0 then 0
   when ems11_tqfr  < 100 then cast(ems11_tqfr / 10 as int)
   when ems11_tqfr is null then -1
  end as ems11_tqfr
 ,case when ems11_vs <= 0 then 0
   when ems11_vs  < 120 then cast(ems11_vs / 20 as int) + 1
   when ems11_vs  >= 120 then 8
   when ems11_vs is null then -1
  end as ems11_vs

 ,case when ems20_fco <= 0 then 0
    when ems20_fco < 40 then cast(ems20_fco / 10 as int ) + 1
    when ems20_fco < 70 then 5
    when ems20_fco < 100 then 6
    when ems20_fco < 130 then 7
    when ems20_fco < 160 then 8
    when ems20_fco >= 160 then 9
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
 ,case when tcu13_cf_tcu_targr is null then -1 
    when tcu13_cf_tcu_targr = 14 then 9
    else cast(tcu13_cf_tcu_targr as int) 
  end as tcu13_cf_tcu_targr
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
    when ems12_tps  < 100 then cast(ems12_tps / 15 as int) + 1
    when ems12_tps  >= 100 then 8
    when ems12_tps is null then -1
  end as ems12_tps
 ,case when ems12_pv_av_can <= 0 then 0
    when ems12_pv_av_can  < 100 then cast(ems12_pv_av_can / 15 as int) + 1
    when ems12_pv_av_can  >= 100 then 8
    when ems12_pv_av_can is null then -1
  end as ems12_pv_av_can
 
 from vcrm_6442267.rg_log_um_dm_tl_f
 ;

## 운행로그 + 14개 센서조합 ID 생성
drop table if exists vcrm_6442267.rg_log_um_f10_factor;
create table vcrm_6442267.rg_log_um_f10_factor STORED AS PARQUET
as
select 
  A.*
from
(
  select
   *
   ,concat(ems11_n,ems11_vs,ems11_swi_igk,ems11_puc_stat,ems12_pv_av_can,ems12_temp_eng,ems14_amp_can,ems_h12_cf_ems_isgstat,ems16_eng_stat,ems16_tqi_target,ems16_tqi,tcu11_n_tc,tcu11_swi_cc,tcu12_cur_gr) as id
  from vcrm_6442267.rg_log_um_dm_tl_f10
  where sale_vehl_cd = 'UM'
) A
where A.id not like '%-1%' -- null 센서 행제거 
;

## 운행로그 패턴생성
drop table if exists vcrm_6442267.rg_log_um_pattern_f10_14;
create table vcrm_6442267.rg_log_um_pattern_f10_14 STORED AS PARQUET
as
select
  A.*
  , A.cnt / B.total_cnt as pct
from
(
select
  sale_vehl_cd
  , id
  , ems11_n,ems11_vs,ems11_swi_igk,ems11_puc_stat,ems12_pv_av_can,ems12_temp_eng,ems14_amp_can,ems_h12_cf_ems_isgstat,ems16_eng_stat,ems16_tqi_target,ems16_tqi,tcu11_n_tc,tcu11_swi_cc,tcu12_cur_gr
  , count(*) as cnt -- 해당 DTC_ID 별 데이터량
from vcrm_6442267.rg_log_um_f10_factor
group by sale_vehl_cd, id, ems11_n,ems11_vs,ems11_swi_igk,ems11_puc_stat,ems12_pv_av_can,ems12_temp_eng,ems14_amp_can,ems_h12_cf_ems_isgstat,ems16_eng_stat,ems16_tqi_target,ems16_tqi,tcu11_n_tc,tcu11_swi_cc,tcu12_cur_gr
) A
left outer join 
  (select sale_vehl_cd, count(*) as total_cnt 
    from vcrm_6442267.rg_log_um_f10_factor 
    group by sale_vehl_cd) B
  on A.sale_vehl_cd = B.sale_vehl_cd
;
-- 280573
-- <= 1 : 92507/280573, 33%
-- <= 10 : 191012/280573, 68%
-- <= 100 : 245276/280573, 87%
-- DTC : 8213/280573, 3%

## 운행로그 + cnt(차종별)
## 최신 또는 특정시점의 패턴을 붙이는 형태
drop table if exists vcrm_6442267.rg_log_um_f10_factor_cnt;
create table vcrm_6442267.rg_log_um_f10_factor_cnt STORED AS PARQUET
as
select
  A.*
  , B.cnt -- 조합 빈도수
from 
  vcrm_6442267.rg_log_um_f10_factor A
left outer join 
  vcrm_6442267.rg_log_um_pattern_f10_14 B
on  A.sale_vehl_cd = B.sale_vehl_cd and A.id = B.id
;
-- 662,088,395

## DTC 테이블 + DTC 패턴 발생빈도
drop table if exists vcrm_6442267.dtc_rt_mnt_svc_um_sigid_stat_14_f10;
create table vcrm_6442267.dtc_rt_mnt_svc_um_sigid_stat_14_f10
as 
select
  A.vin, A.bkdw_cd, A.occu_ymd, A.occu_ctms, A.frame_no, A.id
  , A.id as dtc_id
  , B.cnt
  , B.pct
from vcrm_6442267.dtc_rt_mnt_um_id_14_f10 A
left outer join vcrm_6442267.rg_log_um_pattern_f10_14 B
  on A.id = B.id
;
-- 284158

## um vin 리스트
drop table if exists vcrm_6442267.rg_log_um_vin_list;
create table vcrm_6442267.rg_log_um_vin_list
as
select vin, id from vcrm_6442267.rg_log_um_f10_factor_cnt
group by vin, id
;
--38,854,725


