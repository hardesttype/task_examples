with step_0_masterlist as (
  select distinct
         -- trunc(sysdate) - 180  as application_day,
         contract_begin_date   as application_day,
         siebel_clnt_gid       as client_siebel_gid,
         client_gid            as client_rbo_gid
  from dwh_mart.dm_loan_contract_v ml
  where 1=1
        and ml.crt_gid = 20000000613484706516
), step_1_contract as (
  select --+ parallel(2)
         ml.application_day                      as application_day,
         ml.client_siebel_gid                    as client_siebel_gid,
         ml.client_rbo_gid                       as client_rbo_gid,
         crt.contract_id                         as crt_gid,
         crt.begin_date                          as contract_begin_date,
         nvl(nullif(crt.initial_plan_end_date, date'2399-12-31'),
             nullif(crt.plan_end_date,         date'2399-12-31'))
                                                 as contract_plan_end_date,
         case when nullif(crt.fact_end_date, date'2399-12-31') > ml.application_day
              then null
              else nullif(crt.fact_end_date, date'2399-12-31')
         end                                     as contract_fact_end_date,
         case when lower(prd.risk_group_code)               like '%банковские карты%'
              then 1
              when lower(prd.risk_group_code)               like '%целевой экспресс-кредит%'
              then 2
              else 3
         end                                     as pr_group,
         prd.risk_group_code                     as prod_group,
         prd.risk_subprod_group_code             as sub_prod_name
  from step_0_masterlist                     ml
  join dwh_mart.dp_contract            crt
        on ml.client_siebel_gid = crt.client_id
       and ml.application_day     >  crt.valid_begin_date
       and ml.application_day - 1 <= crt.valid_end_date
       and ml.application_day     >  crt.begin_date
       and crt.tech$row_status = 'A'
  inner -- тип продукта
  join dwh_mart.dp_product             prd
        on prd.product_id = crt.product_id
       and ml.application_day     >  prd.valid_begin_date
       and ml.application_day - 1 <= prd.valid_end_date
       and prd.tech$row_status = 'A'
       and prd.risk_group_code in ('Потребительские нецелевые', 'Банковские карты',
                                   'Целевой экспресс-кредит', 'Целевой кредит')
  where 1=1
), step_2_contract as (
  select --+ parallel(2)
         application_day,
         client_siebel_gid,
         client_rbo_gid,
         crt_gid,
         contract_begin_date,
         contract_plan_end_date,
         contract_fact_end_date,
         nvl(contract_fact_end_date, contract_plan_end_date) as contract_fact_end_date_f,
         pr_group,
         prod_group,
         sub_prod_name,
         case when application_day < nvl(contract_fact_end_date, contract_plan_end_date)
               and application_day < contract_plan_end_date
              then 1
              else 0
         end                      as open_flg
  from step_1_contract            crt
), step_3_contract_features as (
   select --+ parallel(2)
          application_day,
          client_rbo_gid,
          max(contract_begin_date) - min(contract_begin_date) as fst_lst_beg_delta,
          min(application_day - contract_begin_date) as last_deal_ago,
      min(case when pr_group = 1 then application_day - contract_begin_date end) as last_deal_ago_card,
          min(case when pr_group = 2 then application_day - contract_begin_date end) as last_deal_ago_pos,
          min(case when pr_group = 3 then application_day - contract_begin_date end) as last_deal_ago_cash,
          min(case when pr_group = 3 and sub_prod_name like '%Top-up%' then application_day - contract_begin_date end) as last_deal_ago_cashtop,
          min(case when pr_group = 3 and sub_prod_name like '%Продуктовая Корзина%' then application_day - contract_begin_date end) as last_deal_ago_cashfa,
          min(case when open_flg = 1 then application_day - contract_begin_date end) as last_deal_agoa,
      min(case when pr_group = 1 and open_flg = 1 then application_day - contract_begin_date end) as last_deal_ago_carda,
          min(case when pr_group = 2 and open_flg = 1 then application_day - contract_begin_date end) as last_deal_ago_posa,
          min(case when pr_group = 3 and open_flg = 1 then application_day - contract_begin_date end) as last_deal_ago_casha,
          min(case when pr_group = 3 and open_flg = 1 and sub_prod_name like '%Top-up%' then application_day - contract_begin_date end) as last_deal_ago_cashtopa,
          min(case when pr_group = 3 and open_flg = 1 and sub_prod_name like '%Продуктовая Корзина%' then application_day - contract_begin_date end) as last_deal_ago_cashfaa,
          max(application_day - contract_begin_date) as first_deal_ago,
      max(case when pr_group = 1 then application_day - contract_begin_date end) as first_deal_ago_card,
          max(case when pr_group = 2 then application_day - contract_begin_date end) as first_deal_ago_pos,
          max(case when pr_group = 3 then application_day - contract_begin_date end) as first_deal_ago_cash,
          max(case when pr_group = 3 and sub_prod_name like '%Top-up%' then application_day - contract_begin_date end) as first_deal_ago_cashtop,
          max(case when pr_group = 3 and sub_prod_name like '%Продуктовая Корзина%' then application_day - contract_begin_date end) as first_deal_ago_cashfa,
          count(crt_gid) as cnt_all_deals,
          sum(case when pr_group = 1 then 1 end) as cnt_all_deals_card,
          sum(case when pr_group = 2 then 1 end) as cnt_all_deals_pos,
          sum(case when pr_group = 3 then 1 end) as cnt_all_deals_cash,
          sum(case when pr_group = 3 and sub_prod_name like '%Top-up%' then 1 end) as cnt_all_deals_cashtop,
          sum(case when pr_group = 3 and sub_prod_name like '%Продуктовая Корзина%' then 1 end) as cnt_all_deals_cashfa,
          sum(case when application_day between contract_begin_date and nvl(nullif(contract_fact_end_date, date '2399-12-31'), contract_plan_end_date) then 1 else 0 end) as cnt_act_deals,
          sum(case when application_day between contract_begin_date and nvl(nullif(contract_fact_end_date, date '2399-12-31'), contract_plan_end_date) and pr_group = 1 then 1 else 0 end) as cnt_act_deals_card,
          sum(case when application_day between contract_begin_date and nvl(nullif(contract_fact_end_date, date '2399-12-31'), contract_plan_end_date) and pr_group = 2 then 1 else 0 end) as cnt_act_deals_pos,
          sum(case when application_day between contract_begin_date and nvl(nullif(contract_fact_end_date, date '2399-12-31'), contract_plan_end_date) and pr_group = 3 then 1 else 0 end) as cnt_act_deals_cash,
          sum(case when application_day between contract_begin_date and nvl(nullif(contract_fact_end_date, date '2399-12-31'), contract_plan_end_date) and pr_group = 3 and sub_prod_name like '%Top-up%' then 1 else 0 end) as cnt_act_deals_cashtop,
          sum(case when application_day between contract_begin_date and nvl(nullif(contract_fact_end_date, date '2399-12-31'), contract_plan_end_date) and pr_group = 3 and sub_prod_name like '%Продуктовая Корзина%' then 1 else 0 end) as cnt_act_deals_cashfa,
          min(case when contract_fact_end_date <= application_day then nullif(nullif(contract_fact_end_date, to_date ('31.12.2399', 'dd.mm.yyyy')), to_date ('31.12.2050', 'dd.mm.yyyy')) - nullif(nullif(contract_begin_date, to_date ('31.12.2399', 'dd.mm.yyyy')), to_date ('31.12.2050', 'dd.mm.yyyy')) else nullif(nullif(contract_plan_end_date, to_date('31.12.2399', 'dd.mm.yyyy')), to_date('31.12.2050', 'dd.mm.yyyy')) - nullif(nullif(contract_begin_date, to_date('31.12.2399', 'dd.mm.yyyy')), to_date('31.12.2050', 'dd.mm.yyyy')) end) as min_plan_srok,
          max(case when contract_fact_end_date <= application_day then nullif(nullif(contract_fact_end_date, to_date ('31.12.2399', 'dd.mm.yyyy')), to_date ('31.12.2050', 'dd.mm.yyyy')) - nullif(nullif(contract_begin_date, to_date ('31.12.2399', 'dd.mm.yyyy')), to_date ('31.12.2050', 'dd.mm.yyyy')) else nullif(nullif(contract_plan_end_date, to_date('31.12.2399', 'dd.mm.yyyy')), to_date('31.12.2050', 'dd.mm.yyyy')) - nullif(nullif(contract_begin_date, to_date('31.12.2399', 'dd.mm.yyyy')), to_date('31.12.2050', 'dd.mm.yyyy')) end) as max_plan_srok,
          max(case when contract_fact_end_date <= application_day then nullif(nullif(contract_plan_end_date, to_date ('31.12.2399', 'dd.mm.yyyy')), to_date ('31.12.2050', 'dd.mm.yyyy')) - nullif(nullif(contract_fact_end_date, to_date ('31.12.2399', 'dd.mm.yyyy')), to_date ('31.12.2050', 'dd.mm.yyyy')) end) as max_delta_cls_cred,
          sum(case when contract_fact_end_date not in (to_date ('31.12.2399', 'dd.mm.yyyy'), to_date ('31.12.2050', 'dd.mm.yyyy')) and contract_plan_end_date not in (to_date ('31.12.2399', 'dd.mm.yyyy'), to_date ('31.12.2050', 'dd.mm.yyyy')) and contract_fact_end_date < contract_plan_end_date - 30 then 1 else 0 end) as cnt_early_end,
          max(case when application_day - contract_begin_date <= 31 and not (sub_prod_name like '%POS%') then 1 else 0 end) as has_nonpos_in_month,
          max(case when application_day - contract_begin_date <= 180 and nvl(nullif(contract_fact_end_date, date '2399-12-31'), contract_plan_end_date) > application_day and not (sub_prod_name like '%POS%') then 1 else 0 end) as has_nonpos_in_180,
          sum(case when contract_plan_end_date > add_months(application_day, 6) and nvl(nullif(contract_fact_end_date, date '2399-12-31'), contract_plan_end_date) > application_day and prod_group <> 'Банковские карты' then 1 else 0 end) as cnt_actcred_nocc_6m,
          sum(case when prod_group = 'Банковские карты' and application_day between contract_begin_date and nvl(nullif(contract_fact_end_date, date '2399-12-31'), contract_plan_end_date) then 1 else 0 end) as cnt_act_carddeals,
          sum(case when prod_group = 'Банковские карты' then 1 else 0 end) as cnt_all_carddeals,
          min(case when open_flg = 1 then application_day - contract_fact_end_date_f end) as last_deal_enda,
          min(case when pr_group = 1 and open_flg = 1 then application_day - contract_fact_end_date_f end) as last_deal_end_carda,
          min(case when pr_group = 2 and open_flg = 1 then application_day - contract_fact_end_date_f end) as last_deal_end_posa,
          min(case when pr_group = 3 and open_flg = 1 then application_day - contract_fact_end_date_f end) as last_deal_end_casha,
          min(case when pr_group = 3 and open_flg = 1 and sub_prod_name like '%Top-up%' then application_day - contract_fact_end_date_f end) as last_deal_end_cashtopa,
          min(case when pr_group = 3 and open_flg = 1 and sub_prod_name like '%Продуктовая Корзина%' then application_day - contract_fact_end_date_f end) as last_deal_end_cashfaa,
          min(case when pr_group <> 2 and open_flg = 1 then application_day - contract_fact_end_date_f end) as last_deal_end_nonposa,
          min(case when open_flg = 0 then application_day - contract_fact_end_date_f end) as last_deal_endc,
          min(case when pr_group = 1 and open_flg = 0 then application_day - contract_fact_end_date_f end) as last_deal_end_cardc,
          min(case when pr_group = 2 and open_flg = 0 then application_day - contract_fact_end_date_f end) as last_deal_end_posc,
          min(case when pr_group = 3 and open_flg = 0 then application_day - contract_fact_end_date_f end) as last_deal_end_cashc,
          min(case when pr_group = 3 and open_flg = 0 and sub_prod_name like '%Top-up%' then application_day - contract_fact_end_date_f end) as last_deal_end_cashtopc,
          min(case when pr_group = 3 and open_flg = 0 and sub_prod_name like '%Продуктовая Корзина%' then application_day - contract_fact_end_date_f end) as last_deal_end_cashfac,
          min(case when pr_group <> 2 and open_flg = 0then application_day - contract_fact_end_date_f end) as last_deal_end_nonposc,
          min(case when pr_group <> 2 then application_day - contract_begin_date end) as last_deal_ago_nonpos,
          min(case when pr_group <> 2 and open_flg = 1 then application_day - contract_begin_date end) as last_deal_ago_nonposa
   from step_2_contract
   group by application_day, client_rbo_gid
), step_4_turnovers_features as (
  select --+ parallel(2)
          crt.application_day,
          crt.client_rbo_gid,
          case
            when sum(
              case
                when turn.issue_rub_amt > 0 then turn.issue_rub_amt
              end
            ) > 0 then sum(
              case
                when turn.total_repayment_rub_amt > 0 then turn.total_repayment_rub_amt
              end
            ) / sum(
              case
                when turn.issue_rub_amt > 0 then turn.issue_rub_amt
              end
            )
          end as pyrelis_all,
          case
            when sum(
              case
                when turn.issue_rub_amt > 0 and open_flg = 1 then turn.issue_rub_amt
              end
            ) > 0 then sum(
              case
                when turn.total_repayment_rub_amt > 0 and open_flg = 1 then turn.total_repayment_rub_amt
              end
            ) / sum(
              case
                when turn.issue_rub_amt > 0 and open_flg = 1 then turn.issue_rub_amt
              end
            )
          end as pyrelis_all_active,
          case
            when sum(
              case
                when turn.issue_rub_amt > 0 and open_flg = 1 and pr_group = 3 then turn.issue_rub_amt
              end
            ) > 0 then sum(
              case
                when turn.total_repayment_rub_amt > 0 and open_flg = 1 and pr_group = 3 then turn.total_repayment_rub_amt
              end
            ) / sum(
              case
                when turn.issue_rub_amt > 0 and open_flg = 1  and pr_group = 3 then turn.issue_rub_amt
              end
            )
          end as pyrelis_all_active_cash,
          case
            when sum(
              case
                when turn.issue_rub_amt > 0 and open_flg = 1 and pr_group = 1 then turn.issue_rub_amt
              end
            ) > 0 then sum(
              case
                when turn.total_repayment_rub_amt > 0 and open_flg = 1 and pr_group = 1 then turn.total_repayment_rub_amt
              end
            ) / sum(
              case
                when turn.issue_rub_amt > 0 and open_flg = 1  and pr_group = 1 then turn.issue_rub_amt
              end
            )
          end as pyrelis_all_active_card,
          case
            when sum(
              case
                when turn.issue_rub_amt > 0 and open_flg = 1 and pr_group = 2 then turn.issue_rub_amt
              end
            ) > 0 then sum(
              case
                when turn.total_repayment_rub_amt > 0 and open_flg = 1 and pr_group = 2 then turn.total_repayment_rub_amt
              end
            ) / sum(
              case
                when turn.issue_rub_amt > 0 and open_flg = 1  and pr_group = 2 then turn.issue_rub_amt
              end
            )
          end as pyrelis_all_active_pos,
          case
            when sum(
              case
                when turn.issue_rub_amt > 0 and turn.as_of_date >= add_months(crt.application_day, -12) and turn.as_of_date < crt.application_day then turn.issue_rub_amt
              end
            ) > 0 then sum(
              case
                when turn.total_repayment_rub_amt > 0 and turn.as_of_date >= add_months(crt.application_day, -12) and turn.as_of_date < crt.application_day then turn.total_repayment_rub_amt
              end
            ) / sum(
              case
                when turn.issue_rub_amt > 0 and turn.as_of_date >= add_months(crt.application_day, -12) and turn.as_of_date < crt.application_day then turn.issue_rub_amt
              end
            )
          end as pyrelis_all12,
          case
            when sum(
              case
                when turn.issue_rub_amt > 0 and pr_group = 3 and turn.as_of_date >= add_months(crt.application_day, -12) and turn.as_of_date < crt.application_day then turn.issue_rub_amt
              end
            ) > 0 then sum(
              case
                when turn.total_repayment_rub_amt > 0 and pr_group = 3 and turn.as_of_date >= add_months(crt.application_day, -12) and turn.as_of_date < crt.application_day then turn.total_repayment_rub_amt
              end
            ) / sum(
              case
                when turn.issue_rub_amt > 0 and pr_group = 3 and turn.as_of_date >= add_months(crt.application_day, -12) and turn.as_of_date < crt.application_day then turn.issue_rub_amt
              end
            )
          end as pyrelis_all12cash,
          case
            when sum(
              case
                when turn.issue_rub_amt > 0 and pr_group = 1 and turn.as_of_date >= add_months(crt.application_day, -12) and turn.as_of_date < crt.application_day then turn.issue_rub_amt
              end
            ) > 0 then sum(
              case
                when turn.total_repayment_rub_amt > 0 and pr_group = 1 and turn.as_of_date >= add_months(crt.application_day, -12) and turn.as_of_date < crt.application_day then turn.total_repayment_rub_amt
              end
            ) / sum(
              case
                when turn.issue_rub_amt > 0 and pr_group = 1 and turn.as_of_date >= add_months(crt.application_day, -12) and turn.as_of_date < crt.application_day then turn.issue_rub_amt
              end
            )
          end as pyrelis_all12card,
          case
            when sum(
              case
                when turn.issue_rub_amt > 0 and pr_group = 2 and turn.as_of_date >= add_months(crt.application_day, -12) and turn.as_of_date < crt.application_day then turn.issue_rub_amt
              end
            ) > 0 then sum(
              case
                when turn.total_repayment_rub_amt > 0 and pr_group = 2 and turn.as_of_date >= add_months(crt.application_day, -12) and turn.as_of_date < crt.application_day then turn.total_repayment_rub_amt
              end
            ) / sum(
              case
                when turn.issue_rub_amt > 0 and pr_group = 2 and turn.as_of_date >= add_months(crt.application_day, -12) and turn.as_of_date < crt.application_day then turn.issue_rub_amt
              end
            )
          end as pyrelis_all12pos,

          case
            when sum(case
                        when turn.issue_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -6) and turn.as_of_date < crt.application_day then turn.issue_rub_amt
                      end
                     ) > 0 
                              then 
                                sum(
                                    case
                                      when turn.total_repayment_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -6) and turn.as_of_date < crt.application_day  then turn.total_repayment_rub_amt
                                    end
                                  )/ 
                                sum(
                                    case
                                      when turn.issue_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -6) and turn.as_of_date < crt.application_day then turn.issue_rub_amt
                                    end
                                    )
          end as pyrelis_all6,

          case
            when sum(case
                        when turn.issue_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -3) and turn.as_of_date < crt.application_day then turn.issue_rub_amt
                      end
                     ) > 0 
                              then 
                                sum(
                                    case
                                      when turn.total_repayment_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -3) and turn.as_of_date < crt.application_day then turn.total_repayment_rub_amt
                                    end
                                  )/ 
                                sum(
                                    case
                                      when turn.issue_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -3) and turn.as_of_date < crt.application_day then turn.issue_rub_amt
                                    end
                                    )
          end as pyrelis_all3,
            
          case
            when sum(case
                        when turn.issue_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -1) and turn.as_of_date < crt.application_day then turn.issue_rub_amt
                      end
                     ) > 0 
                              then 
                                sum(
                                    case
                                      when turn.total_repayment_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -1) and turn.as_of_date < crt.application_day then turn.total_repayment_rub_amt
                                    end
                                  )/ 
                                sum(
                                    case
                                      when turn.issue_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -1) and turn.as_of_date < crt.application_day then turn.issue_rub_amt
                                    end
                                    )
          end as pyrelis_all1,
      ----
          sum(
            case
              when turn.accrued_interest_rub_amt > 0 and open_flg = 1  then turn.accrued_interest_rub_amt
            end
          ) as sum_accr_int_all_active,
          sum(
            case
              when turn.accrued_interest_rub_amt > 0 and open_flg = 1 and pr_group = 1  then turn.accrued_interest_rub_amt
            end
          ) as sum_accr_int_card_active,
          sum(
            case
              when turn.accrued_interest_rub_amt > 0 and open_flg = 1 and pr_group = 2  then turn.accrued_interest_rub_amt
            end
          ) as sum_accr_int_pos_active,    
          sum(
            case
              when turn.accrued_interest_rub_amt > 0 and open_flg = 1 and pr_group = 3  then turn.accrued_interest_rub_amt
            end
          ) as sum_accr_int_cash_active,    
      ---
          sum(
            case
              when turn.accrued_interest_rub_amt > 0 and open_flg = 1  and turn.as_of_date > add_months(crt.application_day, -1) then turn.accrued_interest_rub_amt
            end
          ) as sum_accr_int_all_active_1m,
          sum(
            case
              when turn.accrued_interest_rub_amt > 0 and open_flg = 1 and pr_group = 1 and turn.as_of_date > add_months(crt.application_day, -1) then turn.accrued_interest_rub_amt
            end
          ) as sum_accr_int_card_active_1m,
          sum(
            case
              when turn.accrued_interest_rub_amt > 0 and open_flg = 1 and pr_group = 2  and turn.as_of_date > add_months(crt.application_day, -1) then turn.accrued_interest_rub_amt
            end
          ) as sum_accr_int_pos_active_1m,    
          sum(
            case
              when turn.accrued_interest_rub_amt > 0 and open_flg = 1 and pr_group = 3 and turn.as_of_date > add_months(crt.application_day, -1) then turn.accrued_interest_rub_amt
            end
          ) as sum_accr_int_cash_active_1m,    


      --------------------------    
      --5y
          stddev(
            case
              when turn.issue_rub_amt > 0 then turn.issue_rub_amt
            end
          ) as std_issue_all_5y,
          min(
            case
              when turn.issue_rub_amt > 0 then turn.issue_rub_amt
            end
          ) as min_issue_all_5y,
          sum(
            case
              when turn.accrued_interest_rub_amt > 0 then turn.accrued_interest_rub_amt
            end
          ) as sum_accr_int_all_5y,
          stddev(
            case
              when turn.accrued_interest_rub_amt > 0 then turn.accrued_interest_rub_amt
            end
          ) as std_accr_int_all_5y,
          avg(
            case
              when turn.accrued_interest_rub_amt > 0 then turn.accrued_interest_rub_amt
            end
          ) as avg_accr_int_all_5y,
          min(
            case
              when turn.accrued_interest_rub_amt > 0 then turn.accrued_interest_rub_amt
            end
          ) as min_accr_int_all_5y,
          max(
            case
              when turn.accrued_interest_rub_amt > 0 then turn.accrued_interest_rub_amt
            end
          ) as max_accr_int_all_5y,
          stddev(
            case
              when turn.principal_repayment_rub_amt > 0 then turn.principal_repayment_rub_amt
            end
          ) as std_mdbtpy_all_5y,
          min(
            case
              when turn.principal_repayment_rub_amt > 0 then turn.principal_repayment_rub_amt
            end
          ) as min_mdbtpy_all_5y,
          max(
            case
              when turn.principal_repayment_rub_amt > 0 then turn.principal_repayment_rub_amt
            end
          ) as max_mdbtpy_all_5y,
          stddev(
            case
              when turn.ovrd_principal_rpmnt_rub_amt > 0 then turn.ovrd_principal_rpmnt_rub_amt
            end
          ) as std_ovrprpy_all_5y,
          sum(
            case
              when turn.principal_delay_rub_amt > 0 then turn.principal_delay_rub_amt
            end
          ) as sum_princ_delay_all_5y,
          stddev(
            case
              when turn.principal_delay_rub_amt > 0 then turn.principal_delay_rub_amt
            end
          ) as std_princ_delay_all_5y,
          avg(
            case
              when turn.principal_delay_rub_amt > 0 then turn.principal_delay_rub_amt
            end
          ) as avg_princ_delay_all_5y,
          min(
            case
              when turn.principal_delay_rub_amt > 0 then turn.principal_delay_rub_amt
            end
          ) as min_princ_delay_all_5y,
          max(
            case
              when turn.principal_delay_rub_amt > 0 then turn.principal_delay_rub_amt
            end
          ) as max_princ_delay_all_5y,
          avg(
            case
              when turn.penalty_repayment_rub_amt + turn.state_duty_repayment_rub_amt > 0 then turn.penalty_repayment_rub_amt + turn.state_duty_repayment_rub_amt
            end
          ) as avg_penalty_repayment_all_5y,
          max(
            case
              when turn.comission_repayment_rub_amt > 0 then turn.comission_repayment_rub_amt
            end
          ) as max_comis_repayment_all_5y,
          stddev(
            case
              when turn.bal_interest_rpmnt_rub_amt + turn.ofbal_interest_rpmnt_rub_amt > 0 then turn.bal_interest_rpmnt_rub_amt + turn.ofbal_interest_rpmnt_rub_amt
            end
          ) as std_intpy_all_5y,
          avg(
            case
              when turn.bal_interest_rpmnt_rub_amt + turn.ofbal_interest_rpmnt_rub_amt > 0 then turn.bal_interest_rpmnt_rub_amt + turn.ofbal_interest_rpmnt_rub_amt
            end
          ) as avg_intpy_all_5y,
          min(
            case
              when turn.bal_interest_rpmnt_rub_amt + turn.ofbal_interest_rpmnt_rub_amt > 0 then turn.bal_interest_rpmnt_rub_amt + turn.ofbal_interest_rpmnt_rub_amt
            end
          ) as min_intpy_all_5y,
          max(
            case
              when turn.bal_interest_rpmnt_rub_amt + turn.ofbal_interest_rpmnt_rub_amt > 0 then turn.bal_interest_rpmnt_rub_amt + turn.ofbal_interest_rpmnt_rub_amt
            end
          ) as max_intpy_all_5y,
      --------------------------    
      --3y
          stddev(
            case
              when turn.issue_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -12*3 ) then turn.issue_rub_amt
            end
          ) as std_issue_all_3y,
          min(
            case
              when turn.issue_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -12*3 ) then turn.issue_rub_amt
            end
          ) as min_issue_all_3y,
          sum(
            case
              when turn.accrued_interest_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -12*3 ) then turn.accrued_interest_rub_amt
            end
          ) as sum_accr_int_all_3y,
          stddev(
            case
              when turn.accrued_interest_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -12*3 ) then turn.accrued_interest_rub_amt
            end
          ) as std_accr_int_all_3y,
          avg(
            case
              when turn.accrued_interest_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -12*3 ) then turn.accrued_interest_rub_amt
            end
          ) as avg_accr_int_all_3y,
          min(
            case
              when turn.accrued_interest_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -12*3 ) then turn.accrued_interest_rub_amt
            end
          ) as min_accr_int_all_3y,
          max(
            case
              when turn.accrued_interest_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -12*3 ) then turn.accrued_interest_rub_amt
            end
          ) as max_accr_int_all_3y,
          stddev(
            case
              when turn.principal_repayment_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -12*3 ) then turn.principal_repayment_rub_amt
            end
          ) as std_mdbtpy_all_3y,
          min(
            case
              when turn.principal_repayment_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -12*3 ) then turn.principal_repayment_rub_amt
            end
          ) as min_mdbtpy_all_3y,
          max(
            case
              when turn.principal_repayment_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -12*3 ) then turn.principal_repayment_rub_amt
            end
          ) as max_mdbtpy_all_3y,
          stddev(
            case
              when turn.ovrd_principal_rpmnt_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -12*3 ) then turn.ovrd_principal_rpmnt_rub_amt
            end
          ) as std_ovrprpy_all_3y,
          sum(
            case
              when turn.principal_delay_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -12*3 ) then turn.principal_delay_rub_amt
            end
          ) as sum_princ_delay_all_3y,
          stddev(
            case
              when turn.principal_delay_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -12*3 ) then turn.principal_delay_rub_amt
            end
          ) as std_princ_delay_all_3y,
          avg(
            case
              when turn.principal_delay_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -12*3 ) then turn.principal_delay_rub_amt
            end
          ) as avg_princ_delay_all_3y,
          min(
            case
              when turn.principal_delay_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -12*3 ) then  turn.principal_delay_rub_amt
            end
          ) as min_princ_delay_all_3y,
          max(
            case
              when turn.principal_delay_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -12*3 ) then  turn.principal_delay_rub_amt
            end
          ) as max_princ_delay_all_3y,
          avg(
            case
              when turn.penalty_repayment_rub_amt + turn.state_duty_repayment_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -12*3 ) then turn.penalty_repayment_rub_amt + turn.state_duty_repayment_rub_amt
            end
          ) as avg_penalty_repayment_all_3y,
          max(
            case
              when turn.comission_repayment_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -12*3 ) then turn.comission_repayment_rub_amt
            end
          ) as max_comis_repayment_all_3y,
          stddev(
            case
              when turn.bal_interest_rpmnt_rub_amt + turn.ofbal_interest_rpmnt_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -12*3 ) then turn.bal_interest_rpmnt_rub_amt + turn.ofbal_interest_rpmnt_rub_amt
            end
          ) as std_intpy_all_3y,
          avg(
            case
              when turn.bal_interest_rpmnt_rub_amt + turn.ofbal_interest_rpmnt_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -12*3 ) then turn.bal_interest_rpmnt_rub_amt + turn.ofbal_interest_rpmnt_rub_amt
            end
          ) as avg_intpy_all_3y,
          min(
            case
              when turn.bal_interest_rpmnt_rub_amt + turn.ofbal_interest_rpmnt_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -12*3 ) then turn.bal_interest_rpmnt_rub_amt + turn.ofbal_interest_rpmnt_rub_amt
            end
          ) as min_intpy_all_3y,
          max(
            case
              when turn.bal_interest_rpmnt_rub_amt + turn.ofbal_interest_rpmnt_rub_amt > 0 and turn.as_of_date > add_months(crt.application_day, -12*3 ) then turn.bal_interest_rpmnt_rub_amt + turn.ofbal_interest_rpmnt_rub_amt
            end
          ) as max_intpy_all_3y
  from step_2_contract                            crt
  join dwh_mart.f_loan_contract_turnovers_v turn
        on crt.crt_gid = turn.contract_gid
       and turn.as_of_date between add_months(crt.application_day, -12*5)
                               and crt.application_day
  group by crt.application_day, crt.client_rbo_gid
), step_5_credit_index_daily as (
  select --+ parallel(2)
         cri.*,
         row_number() over (partition by cri.crdt_gid order by cri.valid_begin_date desc) as rn
  from step_2_contract                            crt
  join dwh_mart.f_deal_credit_index_daily_v cri
        on cri.crdt_gid = crt.crt_gid
       and cri.valid_begin_date < crt.application_day
), step_6_credit_index_daily_features as (
  select --+ parallel(2)
          crt.application_day,
          crt.client_rbo_gid,
          --last_mdbt_all_0s open_flg
              sum(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and cri.rn = 1 then principal_amt
                end
              ) as sum_last_mdbt_all_0s,
              --1
              sum(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and cri.rn = 1 and pr_group = 1 then principal_amt
                end
              ) as sum_last_mdbt_all_0s_card,
              sum(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and cri.rn = 1 and pr_group = 2 then principal_amt
                end
              ) as sum_last_mdbt_all_0s_pos,
              sum(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and cri.rn = 1 and pr_group = 3 then principal_amt
                end
              ) as sum_last_mdbt_all_0s_cash,    

              max(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and cri.rn = 1 then principal_amt
                end
              ) as max_last_mdbt_all_0s,
              --1
              max(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and cri.rn = 1 and pr_group = 1 then principal_amt
                end
              ) as max_last_mdbt_all_0s_card,
              max(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and cri.rn = 1 and pr_group = 2 then principal_amt
                end
              ) as max_last_mdbt_all_0s_pos,
              max(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and cri.rn = 1 and pr_group = 3 then principal_amt
                end
              ) as max_last_mdbt_all_0s_cash,
              
          --last_mdbtovr_all_0s 
              sum(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and cri.rn = 1 then ovrd_principal_amt
                end
              ) as sum_last_mdbtovr_all_0s,
              --1
              sum(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and cri.rn = 1 and pr_group = 1 then ovrd_principal_amt
                end
              ) as sum_last_mdbtovr_all_0s_card,
              sum(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and cri.rn = 1 and pr_group = 2 then ovrd_principal_amt
                end
              ) as sum_last_mdbtovr_all_0s_pos,
              sum(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and cri.rn = 1 and pr_group = 3 then ovrd_principal_amt
                end
              ) as sum_last_mdbtovr_all_0s_cash,    

              max(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and cri.rn = 1 then ovrd_principal_amt
                end
              ) as max_last_mdbtovr_all_0s,
              --1
              max(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and cri.rn = 1 and pr_group = 1 then ovrd_principal_amt
                end
              ) as max_last_mdbtovr_all_0s_card,
              max(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and cri.rn = 1 and pr_group = 2 then ovrd_principal_amt
                end
              ) as max_last_mdbtovr_all_0s_pos,
              max(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and cri.rn = 1 and pr_group = 3 then ovrd_principal_amt
                end
              ) as max_last_mdbtovr_all_0s_cash,
          --last_mdbt_all_0s open_flg
              sum(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and open_flg = 1 and cri.rn = 1 then principal_amt
                end
              ) as sum_last_mdbt_all_0s_a,
              --1
              sum(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and open_flg = 1 and cri.rn = 1 and pr_group = 1 then principal_amt
                end
              ) as sum_last_mdbt_all_0s_card_a,
              sum(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and open_flg = 1 and cri.rn = 1 and pr_group = 2 then principal_amt
                end
              ) as sum_last_mdbt_all_0s_pos_a,
              sum(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and open_flg = 1 and cri.rn = 1 and pr_group = 3 then principal_amt
                end
              ) as sum_last_mdbt_all_0s_cash_a,    

              max(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and open_flg = 1 and cri.rn = 1 then principal_amt
                end
              ) as max_last_mdbt_all_0s_a,
              --1
              max(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and open_flg = 1 and cri.rn = 1 and pr_group = 1 then principal_amt
                end
              ) as max_last_mdbt_all_0s_card_a,
              max(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and open_flg = 1 and cri.rn = 1 and pr_group = 2 then principal_amt
                end
              ) as max_last_mdbt_all_0s_pos_a,
              max(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and open_flg = 1 and cri.rn = 1 and pr_group = 3 then principal_amt
                end
              ) as max_last_mdbt_all_0s_cash_a,
              
          --last_mdbtovr_all_0s 
              sum(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and open_flg = 1 and cri.rn = 1 then ovrd_principal_amt
                end
              ) as sum_last_mdbtovr_all_0s_a,
              --1
              sum(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and open_flg = 1 and cri.rn = 1 and pr_group = 1 then ovrd_principal_amt
                end
              ) as sum_last_mdbtovr_all_0s_card_a,
              sum(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and open_flg = 1 and cri.rn = 1 and pr_group = 2 then ovrd_principal_amt
                end
              ) as sum_last_mdbtovr_all_0s_pos_a,
              sum(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and open_flg = 1 and cri.rn = 1 and pr_group = 3 then ovrd_principal_amt
                end
              ) as sum_last_mdbtovr_all_0s_cash_a,    

              max(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and open_flg = 1 and cri.rn = 1 then ovrd_principal_amt
                end
              ) as max_last_mdbtovr_all_0s_a,
              --1
              max(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and open_flg = 1 and cri.rn = 1 and pr_group = 1 then ovrd_principal_amt
                end
              ) as max_last_mdbtovr_all_0s_card_a,
              max(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and open_flg = 1 and cri.rn = 1 and pr_group = 2 then ovrd_principal_amt
                end
              ) as max_last_mdbtovr_all_0s_pos_a,
              max(
                case
                  when nvl (
                    cri.valid_end_date,
                    to_date ('31.12.2399', 'dd.mm.yyyy')
                  ) >= crt.application_day and open_flg = 1 and cri.rn = 1 and pr_group = 3 then ovrd_principal_amt
                end
              ) as max_last_mdbtovr_all_0s_cash_a,
        --max_mdbt_all_0s
          max(
            case
            when nvl (
              cri.valid_end_date,
              to_date ('31.12.2399', 'dd.mm.yyyy')
            ) >= crt.application_day then principal_amt
            end
          ) as max_mdbt_all_0s,
          --1
          max(
            case
            when nvl (
              cri.valid_end_date,
              to_date ('31.12.2399', 'dd.mm.yyyy')
            ) >= crt.application_day and pr_group = 1 then principal_amt
            end
          ) as max_mdbt_all_0s_card,
          max(
            case
            when nvl (
              cri.valid_end_date,
              to_date ('31.12.2399', 'dd.mm.yyyy')
            ) >= crt.application_day and pr_group = 2 then principal_amt
            end
          ) as max_mdbt_all_0s_pos,
          max(
            case
            when nvl (
              cri.valid_end_date,
              to_date ('31.12.2399', 'dd.mm.yyyy')
            ) >= crt.application_day and pr_group = 3 then principal_amt
            end
          ) as max_mdbt_all_0s_cash,
        --max_mdbt_all_0s open_flg   
          max(
            case
            when nvl (
              cri.valid_end_date,
              to_date ('31.12.2399', 'dd.mm.yyyy')
            ) >= crt.application_day and open_flg = 1 then principal_amt
            end
          ) as max_mdbt_all_0s_a,
          --1
          max(
            case
            when nvl (
              cri.valid_end_date,
              to_date ('31.12.2399', 'dd.mm.yyyy')
            ) >= crt.application_day and pr_group = 1 and open_flg = 1 then principal_amt
            end
          ) as max_mdbt_all_0s_card_a,
          max(
            case
            when nvl (
              cri.valid_end_date,
              to_date ('31.12.2399', 'dd.mm.yyyy')
            ) >= crt.application_day and pr_group = 2  and open_flg = 1 then principal_amt
            end
          ) as max_mdbt_all_0s_pos_a,
          max(
            case
            when nvl (
              cri.valid_end_date,
              to_date ('31.12.2399', 'dd.mm.yyyy')
            ) >= crt.application_day and pr_group = 3  and open_flg = 1 then principal_amt
            end
          ) as max_mdbt_all_0s_cash_a,    
        -- max_mdbtovr_all_0s  
          max(
            case
            when nvl (
              cri.valid_end_date,
              to_date ('31.12.2399', 'dd.mm.yyyy')
            ) >= crt.application_day then ovrd_principal_amt
            end
          ) as max_mdbtovr_all_0s,
          --1
           max(
            case
            when nvl (
              cri.valid_end_date,
              to_date ('31.12.2399', 'dd.mm.yyyy')
            ) >= crt.application_day and pr_group = 1 then ovrd_principal_amt
            end
          ) as max_mdbtovr_all_0s_card,
           max(
            case
            when nvl (
              cri.valid_end_date,
              to_date ('31.12.2399', 'dd.mm.yyyy')
            ) >= crt.application_day and pr_group = 2 then ovrd_principal_amt
            end
          ) as max_mdbtovr_all_0s_pos,
           max(
            case
            when nvl (
              cri.valid_end_date,
              to_date ('31.12.2399', 'dd.mm.yyyy')
            ) >= crt.application_day and pr_group = 3 then ovrd_principal_amt
            end
          ) as max_mdbtovr_all_0s_cash,
        --a
        -- max_mdbtovr_all_0s  
          max(
            case
            when nvl (
              cri.valid_end_date,
              to_date ('31.12.2399', 'dd.mm.yyyy')
            ) >= crt.application_day  and open_flg = 1 then ovrd_principal_amt
            end
          ) as max_mdbtovr_all_0s_a,
          --1
           max(
            case
            when nvl (
              cri.valid_end_date,
              to_date ('31.12.2399', 'dd.mm.yyyy')
            ) >= crt.application_day and pr_group = 1  and open_flg = 1 then ovrd_principal_amt
            end
          ) as max_mdbtovr_all_0s_card_a,
           max(
            case
            when nvl (
              cri.valid_end_date,
              to_date ('31.12.2399', 'dd.mm.yyyy')
            ) >= crt.application_day and pr_group = 2  and open_flg = 1 then ovrd_principal_amt
            end
          ) as max_mdbtovr_all_0s_pos_a,
           max(
            case
            when nvl (
              cri.valid_end_date,
              to_date ('31.12.2399', 'dd.mm.yyyy')
            ) >= crt.application_day and pr_group = 3  and open_flg = 1 then ovrd_principal_amt
            end
          ) as max_mdbtovr_all_0s_cash_a,
        ---max_mdbt_all_ever
          max (principal_amt) as max_mdbt_all_ever,
          --1
          max (case when pr_group = 1 then principal_amt end) as max_mdbt_all_ever_card,
          max (case when pr_group = 2 then principal_amt end) as max_mdbt_all_ever_pos,
          max (case when pr_group = 3 then principal_amt end) as max_mdbt_all_ever_cash,
        --a
        ---max_mdbt_all_ever 
          max (case when open_flg = 1 then principal_amt end ) as max_mdbt_all_ever_a,
          --1
          max (case when pr_group = 1 and open_flg = 1 then principal_amt end) as max_mdbt_all_ever_card_a,
          max (case when pr_group = 2 and open_flg = 1 then principal_amt end) as max_mdbt_all_ever_pos_a,
          max (case when pr_group = 3 and open_flg = 1 then principal_amt end) as max_mdbt_all_ever_cash_a,
        --max_mdbtovr_all_ever    
          max (ovrd_principal_amt) as max_mdbtovr_all_ever,
          --1
          max (case when pr_group = 1 then ovrd_principal_amt end) as max_mdbtovr_all_ever_card,
          max (case when pr_group = 2 then ovrd_principal_amt end) as max_mdbtovr_all_ever_pos,
          max (case when pr_group = 3 then ovrd_principal_amt end) as max_mdbtovr_all_ever_cash,
        --a
          max (case when open_flg = 1 then ovrd_principal_amt end) as max_mdbtovr_all_ever_a,
          --1
          max (case when pr_group = 1 and open_flg = 1 then ovrd_principal_amt end) as max_mdbtovr_all_ever_card_a,
          max (case when pr_group = 2 and open_flg = 1 then ovrd_principal_amt end) as max_mdbtovr_all_ever_pos_a,
          max (case when pr_group = 3 and open_flg = 1 then ovrd_principal_amt end) as max_mdbtovr_all_ever_cash_a
        
  from step_2_contract                            crt
  join step_5_credit_index_daily                  cri
        on cri.crdt_gid = crt.crt_gid
       and cri.valid_begin_date < crt.application_day
  group by crt.application_day, crt.client_rbo_gid
), step_7_overdue_index_features as (
  select --+ parallel(2)
          crt.application_day,
          crt.client_rbo_gid,
          --max_days3_0
              max (
                nvl(overdue_end_date, application_day) - overdue_begin_date
              ) as max_days3_0,
              --
               max (  case when pr_group = 1  then  nvl(overdue_end_date, application_day) - overdue_begin_date end  ) as max_days3_0_card,
               max (  case when pr_group = 2  then  nvl(overdue_end_date, application_day) - overdue_begin_date end  ) as max_days3_0_pos,
               max (  case when pr_group = 3  then  nvl(overdue_end_date, application_day) - overdue_begin_date end  ) as max_days3_0_cash,
               max (  case when pr_group = 3 and sub_prod_name like '%Top-up%'  then  nvl(overdue_end_date, application_day) - overdue_begin_date end  ) as max_days3_0_cashtop,
               max (  case when pr_group = 3 and sub_prod_name like '%Продуктовая Корзина%'  then  nvl(overdue_end_date, application_day) - overdue_begin_date end  ) as max_days3_0_cashtfa,
          --max_days3_3
              max (
                case
                  when nvl (overdue_end_date, application_day) >= add_months (application_day, -3) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_3,
          --
              max (
                case
                  when pr_group = 1 and nvl (overdue_end_date, application_day) >= add_months (application_day, -3) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_3_card,
              max (
                case
                  when pr_group = 2 and nvl (overdue_end_date, application_day) >= add_months (application_day, -3) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_3_pos,
              max (
                case
                  when pr_group = 3 and nvl (overdue_end_date, application_day) >= add_months (application_day, -3) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_3_cash,
              max (
                case
                  when pr_group = 3 and sub_prod_name like '%Top-up%' and nvl (overdue_end_date, application_day) >= add_months (application_day, -3) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_3_cashtop,
              max (
                case
                  when pr_group = 3 and sub_prod_name like '%Продуктовая Корзина%' and nvl (overdue_end_date, application_day) >= add_months (application_day, -3) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_3_cashfa,
          ---max_days3_6
             max (
                case
                  when nvl (overdue_end_date, application_day) >= add_months (application_day, -6) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_6,
          --
             max (
                case
                  when pr_group = 1 and nvl (overdue_end_date, application_day) >= add_months (application_day, -6) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_6_card,
             max (
                case
                  when pr_group = 2 and nvl (overdue_end_date, application_day) >= add_months (application_day, -6) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_6_pos,
             max (
                case
                  when pr_group = 3 and nvl (overdue_end_date, application_day) >= add_months (application_day, -6) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_6_cash,
             max (
                case
                  when pr_group = 3 and sub_prod_name like '%Top-up%' and nvl (overdue_end_date, application_day) >= add_months (application_day, -6) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_6_cashtop,
             max (
                case
                  when pr_group = 3 and sub_prod_name like '%Продуктовая Корзина%' and nvl (overdue_end_date, application_day) >= add_months (application_day, -6) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_6_cashfa,
          ---max_days3_9
              max (
                case
                  when nvl (overdue_end_date, application_day) >= add_months (application_day, -9) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_9,
          --
             max (
                case
                  when pr_group = 1 and nvl (overdue_end_date, application_day) >= add_months (application_day, -9) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_9_card,
             max (
                case
                  when pr_group = 2 and nvl (overdue_end_date, application_day) >= add_months (application_day, -9) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_9_pos,
             max (
                case
                  when pr_group = 3 and nvl (overdue_end_date, application_day) >= add_months (application_day, -9) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_9_cash,
             max (
                case
                  when pr_group = 3 and sub_prod_name like '%Top-up%' and nvl (overdue_end_date, application_day) >= add_months (application_day, -9) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_9_cashtop,
             max (
                case
                  when pr_group = 3 and sub_prod_name like '%Продуктовая Корзина%' and nvl (overdue_end_date, application_day) >= add_months (application_day, -9) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_9_cashfa,
          --max_days3_12
              max (
                case
                  when nvl (overdue_end_date, application_day) >= add_months (application_day, -12) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_12,
          ---
             max (
                case
                  when pr_group = 1 and nvl (overdue_end_date, application_day) >= add_months (application_day, -12) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_12_card,
             max (
                case
                  when pr_group = 2 and nvl (overdue_end_date, application_day) >= add_months (application_day, -12) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_12_pos,
             max (
                case
                  when pr_group = 3 and nvl (overdue_end_date, application_day) >= add_months (application_day, -12) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_12_cash,
             max (
                case
                  when pr_group = 3 and sub_prod_name like '%Top-up%' and nvl (overdue_end_date, application_day) >= add_months (application_day, -12) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_12_cashtop,
             max (
                case
                  when pr_group = 3 and sub_prod_name like '%Продуктовая Корзина%' and nvl (overdue_end_date, application_day) >= add_months (application_day, -12) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_12_cashfa,
          --max_days3_24
              max (
                case
                  when nvl (overdue_end_date, application_day) >= add_months (application_day, -24) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_24,
          ---
             max (
                case
                  when pr_group = 1 and nvl (overdue_end_date, application_day) >= add_months (application_day, -24) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_24_card,
             max (
                case
                  when pr_group = 2 and nvl (overdue_end_date, application_day) >= add_months (application_day, -24) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_24_pos,
             max (
                case
                  when pr_group = 3 and nvl (overdue_end_date, application_day) >= add_months (application_day, -24) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_24_cash,
             max (
                case
                  when pr_group = 3 and sub_prod_name like '%Top-up%' and nvl (overdue_end_date, application_day) >= add_months (application_day, -24) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_24_cashtop,
             max (
                case
                  when pr_group = 3 and sub_prod_name like '%Продуктовая Корзина%' and nvl (overdue_end_date, application_day) >= add_months (application_day, -24) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_24_cashfa,
          --max_days3_36
            max (
                case
                  when nvl (overdue_end_date, application_day) >= add_months (application_day, -36) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_36,
          ---
             max (
                case
                  when pr_group = 1 and nvl (overdue_end_date, application_day) >= add_months (application_day, -36) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_36_card,
             max (
                case
                  when pr_group = 2 and nvl (overdue_end_date, application_day) >= add_months (application_day, -36) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_36_pos,
             max (
                case
                  when pr_group = 3 and nvl (overdue_end_date, application_day) >= add_months (application_day, -36) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_36_cash,
             max (
                case
                  when pr_group = 3 and sub_prod_name like '%Top-up%' and nvl (overdue_end_date, application_day) >= add_months (application_day, -36) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_36_cashtop,
             max (
                case
                  when pr_group = 3 and sub_prod_name like '%Продуктовая Корзина%' and nvl (overdue_end_date, application_day) >= add_months (application_day, -36) then nvl (overdue_end_date, application_day) - overdue_begin_date
                  else 0
                end
              ) as max_days3_36_cashfa,
          --
              max (
                nvl (overdue_end_date, application_day) - overdue_begin_date
              ) as max_days3_ever,
          --min_days_to_ovrd
              min (overdue_begin_date - contract_begin_date) as min_days_to_ovrd,
              --
              min (  case when pr_group = 1  then  (overdue_begin_date - contract_begin_date) end  ) as min_days_to_ovrd_card,
              min (  case when pr_group = 2  then  (overdue_begin_date - contract_begin_date) end  ) as min_days_to_ovrd_pos,
              min (  case when pr_group = 3  then  (overdue_begin_date - contract_begin_date) end  ) as min_days_to_ovrd_cash,
              min (  case when pr_group = 3 and sub_prod_name like '%Top-up%'  then  (overdue_begin_date - contract_begin_date) end  ) as min_days_to_ovrd_cashtop,
              min (  case when pr_group = 3 and sub_prod_name like '%Продуктовая Корзина%'  then  (overdue_begin_date - contract_begin_date) end  ) as min_days_to_ovrd_cashtfa,
          --cnt_ovrd_3    
              sum (
                case
                  when nvl (overdue_end_date, application_day) >= add_months (application_day, -3) then 1
                  else 0
                end
              ) as cnt_ovrd_3,
              --
              sum (
                case
                  when pr_group = 1 and nvl (overdue_end_date, application_day) >= add_months (application_day, -3) then 1
                  else 0
                end
              ) as cnt_ovrd_3_card,
              sum (
                case
                  when pr_group = 2 and nvl (overdue_end_date, application_day) >= add_months (application_day, -3) then 1
                  else 0
                end
              ) as cnt_ovrd_3_pos,
              sum (
                case
                  when pr_group = 3 and nvl (overdue_end_date, application_day) >= add_months (application_day, -3) then 1
                  else 0
                end
              ) as cnt_ovrd_3_cash,
              sum (
                case
                  when pr_group = 3 and sub_prod_name like '%Top-up%' and nvl (overdue_end_date, application_day) >= add_months (application_day, -3) then 1
                  else 0
                end
              ) as cnt_ovrd_3_cashtop,
              sum (
                case
                  when pr_group = 3 and sub_prod_name like '%Продуктовая Корзина%'  and nvl (overdue_end_date, application_day) >= add_months (application_day, -3) then 1
                  else 0
                end
              ) as cnt_ovrd_3_cashfa,
          --cnt_ovrd_6 
              sum (
                case
                  when nvl (overdue_end_date, application_day) >= add_months (application_day, -6) then 1
                  else 0
                end
              ) as cnt_ovrd_6,
             --
              sum (
                case
                  when pr_group = 1 and nvl (overdue_end_date, application_day) >= add_months (application_day, -6) then 1
                  else 0
                end
              ) as cnt_ovrd_6_card,
              sum (
                case
                  when pr_group = 2 and nvl (overdue_end_date, application_day) >= add_months (application_day, -6) then 1
                  else 0
                end
              ) as cnt_ovrd_6_pos,
              sum (
                case
                  when pr_group = 3 and nvl (overdue_end_date, application_day) >= add_months (application_day, -6) then 1
                  else 0
                end
              ) as cnt_ovrd_6_cash,
              sum (
                case
                  when pr_group = 3 and sub_prod_name like '%Top-up%' and nvl (overdue_end_date, application_day) >= add_months (application_day, -6) then 1
                  else 0
                end
              ) as cnt_ovrd_6_cashtop,
              sum (
                case
                  when pr_group = 3 and sub_prod_name like '%Продуктовая Корзина%'  and nvl (overdue_end_date, application_day) >= add_months (application_day, -6) then 1
                  else 0
                end
              ) as cnt_ovrd_6_cashfa,   
          --cnt_ovrd_9   
              sum (
                case
                  when nvl (overdue_end_date, application_day) >= add_months (application_day, -9) then 1
                  else 0
                end
              ) as cnt_ovrd_9,
          --
              sum (
                case
                  when pr_group = 1 and nvl (overdue_end_date, application_day) >= add_months (application_day, -9) then 1
                  else 0
                end
              ) as cnt_ovrd_9_card,
              sum (
                case
                  when pr_group = 2 and nvl (overdue_end_date, application_day) >= add_months (application_day, -9) then 1
                  else 0
                end
              ) as cnt_ovrd_9_pos,
              sum (
                case
                  when pr_group = 3 and nvl (overdue_end_date, application_day) >= add_months (application_day, -9) then 1
                  else 0
                end
              ) as cnt_ovrd_9_cash,
              sum (
                case
                  when pr_group = 3 and sub_prod_name like '%Top-up%' and nvl (overdue_end_date, application_day) >= add_months (application_day, -9) then 1
                  else 0
                end
              ) as cnt_ovrd_9_cashtop,
              sum (
                case
                  when pr_group = 3 and sub_prod_name like '%Продуктовая Корзина%'  and nvl (overdue_end_date, application_day) >= add_months (application_day, -9) then 1
                  else 0
                end
              ) as cnt_ovrd_9_cashfa,
          --cnt_ovrd_12
              sum (
                case
                  when nvl (overdue_end_date, application_day) >= add_months (application_day, -12) then 1
                  else 0
                end
              ) as cnt_ovrd_12,
              --
              sum (
                case
                  when pr_group = 1 and nvl (overdue_end_date, application_day) >= add_months (application_day, -12) then 1
                  else 0
                end
              ) as cnt_ovrd_12_card,
              sum (
                case
                  when pr_group = 2 and nvl (overdue_end_date, application_day) >= add_months (application_day, -12) then 1
                  else 0
                end
              ) as cnt_ovrd_12_pos,
              sum (
                case
                  when pr_group = 3 and nvl (overdue_end_date, application_day) >= add_months (application_day, -12) then 1
                  else 0
                end
              ) as cnt_ovrd_12_cash,
              sum (
                case
                  when pr_group = 3 and sub_prod_name like '%Top-up%' and nvl (overdue_end_date, application_day) >= add_months (application_day, -12) then 1
                  else 0
                end
              ) as cnt_ovrd_12_cashtop,
              sum (
                case
                  when pr_group = 3 and sub_prod_name like '%Продуктовая Корзина%'  and nvl (overdue_end_date, application_day) >= add_months (application_day, -12) then 1
                  else 0
                end
              ) as cnt_ovrd_12_cashfa,
          --cnt_ovrd_24    
              sum (
                case
                  when nvl (overdue_end_date, application_day) >= add_months (application_day, -24) then 1
                  else 0
                end
              ) as cnt_ovrd_24,
          --
              sum (
                case
                  when pr_group = 1 and nvl (overdue_end_date, application_day) >= add_months (application_day, -24) then 1
                  else 0
                end
              ) as cnt_ovrd_24_card,
              sum (
                case
                  when pr_group = 2 and nvl (overdue_end_date, application_day) >= add_months (application_day, -24) then 1
                  else 0
                end
              ) as cnt_ovrd_24_pos,
              sum (
                case
                  when pr_group = 3 and nvl (overdue_end_date, application_day) >= add_months (application_day, -24) then 1
                  else 0
                end
              ) as cnt_ovrd_24_cash,
              sum (
                case
                  when pr_group = 3 and sub_prod_name like '%Top-up%' and nvl (overdue_end_date, application_day) >= add_months (application_day, -24) then 1
                  else 0
                end
              ) as cnt_ovrd_24_cashtop,
              sum (
                case
                  when pr_group = 3 and sub_prod_name like '%Продуктовая Корзина%'  and nvl (overdue_end_date, application_day) >= add_months (application_day, -24) then 1
                  else 0
                end
              ) as cnt_ovrd_24_cashfa,
          --cnt_ovrd_36
            sum (
                case
                  when nvl (overdue_end_date, application_day) >= add_months (application_day, -36) then 1
                  else 0
                end
              ) as cnt_ovrd_36,
          --
              sum (
                case
                  when pr_group = 1 and nvl (overdue_end_date, application_day) >= add_months (application_day, -36) then 1
                  else 0
                end
              ) as cnt_ovrd_36_card,
              sum (
                case
                  when pr_group = 2 and nvl (overdue_end_date, application_day) >= add_months (application_day, -36) then 1
                  else 0
                end
              ) as cnt_ovrd_36_pos,
              sum (
                case
                  when pr_group = 3 and nvl (overdue_end_date, application_day) >= add_months (application_day, -36) then 1
                  else 0
                end
              ) as cnt_ovrd_36_cash,
              sum (
                case
                  when pr_group = 3 and sub_prod_name like '%Top-up%' and nvl (overdue_end_date, application_day) >= add_months (application_day, -36) then 1
                  else 0
                end
              ) as cnt_ovrd_36_cashtop,
              sum (
                case
                  when pr_group = 3 and sub_prod_name like '%Продуктовая Корзина%'  and nvl (overdue_end_date, application_day) >= add_months (application_day, -36) then 1
                  else 0
                end
              ) as cnt_ovrd_36_cashfa,
              count (1) as cnt_ovrd_ever
  from step_2_contract                            crt
  join dwh_mart.dm_overdue_index            ovd 
        on ovd.contract_gid = crt.crt_gid
       and tech$end_date = to_date('31.12.2399', 'dd.mm.yyyy')
       and overdue_begin_date < application_day
       and overdue_type_cd = 11
  group by crt.application_day, crt.client_rbo_gid
), step_8_deferments_tmp as (
  select dp_crt.parent_contract_id      as crt_gid,
         trunc(dp_crt.sign_date)        as sign_date,
         dp_crt.valid_begin_date        as valid_begin_date,
         dp_crt.valid_end_date          as valid_end_date,
         dp_typ.contract_type_name_txt  as contract_type_name_txt
  from step_2_contract                       crt
  join dwh_mart.dp_additional_contract dp_crt
        on dp_crt.parent_contract_id = crt.crt_gid
       and crt.application_day > nvl(trunc(dp_crt.sign_date), dp_crt.valid_begin_date)
  join dwh_mart.dp_contract_type dp_typ
        on dp_typ.contract_type_id = dp_crt.contract_type_id
       and dp_typ.parent_contract_type_id in (4101)
       and dp_crt.tech$row_status = 'A'
       and dp_typ.tech$row_status = 'A'
  union
  select dp_prp.contract_id         as crt_gid,
         cast(null as date)         as sign_date,
         dp_prp.valid_begin_date    as valid_begin_date,
         dp_prp.valid_end_date      as valid_end_date,
         dp_typ.property_type_code  as contract_type_name_txt
  from step_2_contract                       crt
  join dwh_mart.dp_contract_properties dp_prp
        on dp_prp.contract_id = crt.crt_gid
       and crt.application_day > dp_prp.valid_begin_date
  join dwh_mart.dp_property_type dp_typ
        on dp_typ.property_type_id = dp_prp.property_type_id
       and dp_prp.property_type_id = 1252
       and dp_prp.value_number = 1
       and dp_prp.tech$row_status = 'A'
       and dp_typ.tech$row_status = 'A'
), step_9_deferments as (
  select crt_gid                             as crt_gid,
         min(sign_date)                      as sign_date,
         valid_begin_date                    as valid_begin_date,
         max(valid_end_date)                 as valid_end_date,
         max(contract_type_name_txt) keep (
           dense_rank first order by sign_date
         )                                   as contract_type_name_txt
  from step_8_deferments_tmp
  group by crt_gid, valid_begin_date
), step_10_deferments_features as (
  select --+ parallel(2)
          crt.application_day,
          crt.client_rbo_gid,
          min(application_day - def.valid_end_date) / 30                     as month_from_last_deferment_end,
          max(application_day - def.valid_end_date) / 30                     as month_from_first_deferment_end,
          min(application_day - nvl(def.sign_date, def.valid_end_date)) / 30 as month_from_last_deferment_start,
          max(application_day - nvl(def.sign_date, def.valid_end_date)) / 30 as month_from_first_deferment_start,
          max(def.contract_type_name_txt) keep ( -- тип последних каникул
              dense_rank last order by def.valid_end_date
          )                                                                  as last_deferment_type
  from step_2_contract                            crt
  join step_9_deferments                          def 
       on def.crt_gid = crt.crt_gid
  group by crt.application_day, crt.client_rbo_gid
)
select --+ parallel(2)
       ml.client_rbo_gid,  -- client_id
       ml.application_day, -- rep_date
       crt.fst_lst_beg_delta,
       crt.last_deal_ago,
       crt.last_deal_ago_card,
       crt.last_deal_ago_pos,
       crt.last_deal_ago_cash,
       crt.last_deal_ago_cashtop,
       crt.last_deal_ago_cashfa,
       crt.last_deal_agoa,
       crt.last_deal_ago_carda,
       crt.last_deal_ago_posa,
       crt.last_deal_ago_casha,
       crt.last_deal_ago_cashtopa,
       crt.last_deal_ago_cashfaa,
       crt.first_deal_ago,
       crt.first_deal_ago_card,
       crt.first_deal_ago_pos,
       crt.first_deal_ago_cash,
       crt.first_deal_ago_cashtop,
       crt.first_deal_ago_cashfa,
       crt.cnt_all_deals,
       crt.cnt_all_deals_card,
       crt.cnt_all_deals_pos,
       crt.cnt_all_deals_cash,
       crt.cnt_all_deals_cashtop,
       crt.cnt_all_deals_cashfa,
       crt.cnt_act_deals,
       crt.cnt_act_deals_card,
       crt.cnt_act_deals_pos,
       crt.cnt_act_deals_cash,
       crt.cnt_act_deals_cashtop,
       crt.cnt_act_deals_cashfa,
       crt.min_plan_srok,
       crt.max_plan_srok,
       crt.max_delta_cls_cred,
       crt.cnt_early_end,
       crt.has_nonpos_in_month,
       crt.has_nonpos_in_180,
       crt.cnt_actcred_nocc_6m,
       crt.cnt_act_carddeals,
       crt.cnt_all_carddeals,
       crt.last_deal_enda,
       crt.last_deal_end_carda,
       crt.last_deal_end_posa,
       crt.last_deal_end_casha,
       crt.last_deal_end_cashtopa,
       crt.last_deal_end_cashfaa,
       crt.last_deal_end_nonposa,
       crt.last_deal_endc,
       crt.last_deal_end_cardc,
       crt.last_deal_end_posc,
       crt.last_deal_end_cashc,
       crt.last_deal_end_cashtopc,
       crt.last_deal_end_cashfac,
       crt.last_deal_end_nonposc,
       crt.last_deal_ago_nonpos,
       crt.last_deal_ago_nonposa,

       trn.pyrelis_all,
       trn.pyrelis_all_active,
       trn.pyrelis_all_active_cash,
       trn.pyrelis_all_active_card,
       trn.pyrelis_all_active_pos,
       trn.pyrelis_all12,
       trn.pyrelis_all12cash,
       trn.pyrelis_all12card,
       trn.pyrelis_all12pos,
       trn.pyrelis_all6,
       trn.pyrelis_all3,
       trn.pyrelis_all1,
       trn.sum_accr_int_all_active,
       trn.sum_accr_int_card_active,
       trn.sum_accr_int_pos_active,
       trn.sum_accr_int_cash_active,
       trn.sum_accr_int_all_active_1m,
       trn.sum_accr_int_card_active_1m,
       trn.sum_accr_int_pos_active_1m,
       trn.sum_accr_int_cash_active_1m,
       trn.std_issue_all_5y,
       trn.min_issue_all_5y,
       trn.sum_accr_int_all_5y,
       trn.std_accr_int_all_5y,
       trn.avg_accr_int_all_5y,
       trn.min_accr_int_all_5y,
       trn.max_accr_int_all_5y,
       trn.std_mdbtpy_all_5y,
       trn.min_mdbtpy_all_5y,
       trn.max_mdbtpy_all_5y,
       trn.std_ovrprpy_all_5y,
       trn.sum_princ_delay_all_5y,
       trn.std_princ_delay_all_5y,
       trn.avg_princ_delay_all_5y,
       trn.min_princ_delay_all_5y,
       trn.max_princ_delay_all_5y,
       trn.avg_penalty_repayment_all_5y,
       trn.max_comis_repayment_all_5y,
       trn.std_intpy_all_5y,
       trn.avg_intpy_all_5y,
       trn.min_intpy_all_5y,
       trn.max_intpy_all_5y,
       trn.std_issue_all_3y,
       trn.min_issue_all_3y,
       trn.sum_accr_int_all_3y,
       trn.std_accr_int_all_3y,
       trn.avg_accr_int_all_3y,
       trn.min_accr_int_all_3y,
       trn.max_accr_int_all_3y,
       trn.std_mdbtpy_all_3y,
       trn.min_mdbtpy_all_3y,
       trn.max_mdbtpy_all_3y,
       trn.std_ovrprpy_all_3y,
       trn.sum_princ_delay_all_3y,
       trn.std_princ_delay_all_3y,
       trn.avg_princ_delay_all_3y,
       trn.min_princ_delay_all_3y,
       trn.max_princ_delay_all_3y,
       trn.avg_penalty_repayment_all_3y,
       trn.max_comis_repayment_all_3y,
       trn.std_intpy_all_3y,
       trn.avg_intpy_all_3y,
       trn.min_intpy_all_3y,
       trn.max_intpy_all_3y,

       cri.sum_last_mdbt_all_0s,
       cri.sum_last_mdbt_all_0s_card,
       cri.sum_last_mdbt_all_0s_pos,
       cri.sum_last_mdbt_all_0s_cash,
       cri.max_last_mdbt_all_0s,
       cri.max_last_mdbt_all_0s_card,
       cri.max_last_mdbt_all_0s_pos,
       cri.max_last_mdbt_all_0s_cash,
       cri.sum_last_mdbtovr_all_0s,
       cri.sum_last_mdbtovr_all_0s_card,
       cri.sum_last_mdbtovr_all_0s_pos,
       cri.sum_last_mdbtovr_all_0s_cash,
       cri.max_last_mdbtovr_all_0s,
       cri.max_last_mdbtovr_all_0s_card,
       cri.max_last_mdbtovr_all_0s_pos,
       cri.max_last_mdbtovr_all_0s_cash,
       cri.sum_last_mdbt_all_0s_a,
       cri.sum_last_mdbt_all_0s_card_a,
       cri.sum_last_mdbt_all_0s_pos_a,
       cri.sum_last_mdbt_all_0s_cash_a,
       cri.max_last_mdbt_all_0s_a,
       cri.max_last_mdbt_all_0s_card_a,
       cri.max_last_mdbt_all_0s_pos_a,
       cri.max_last_mdbt_all_0s_cash_a,
       cri.sum_last_mdbtovr_all_0s_a,
       cri.sum_last_mdbtovr_all_0s_card_a,
       cri.sum_last_mdbtovr_all_0s_pos_a,
       cri.sum_last_mdbtovr_all_0s_cash_a,
       cri.max_last_mdbtovr_all_0s_a,
       cri.max_last_mdbtovr_all_0s_card_a,
       cri.max_last_mdbtovr_all_0s_pos_a,
       cri.max_last_mdbtovr_all_0s_cash_a,
       cri.max_mdbt_all_0s,
       cri.max_mdbt_all_0s_card,
       cri.max_mdbt_all_0s_pos,
       cri.max_mdbt_all_0s_cash,
       cri.max_mdbt_all_0s_a,
       cri.max_mdbt_all_0s_card_a,
       cri.max_mdbt_all_0s_pos_a,
       cri.max_mdbt_all_0s_cash_a,
       cri.max_mdbtovr_all_0s,
       cri.max_mdbtovr_all_0s_card,
       cri.max_mdbtovr_all_0s_pos,
       cri.max_mdbtovr_all_0s_cash,
       cri.max_mdbtovr_all_0s_a,
       cri.max_mdbtovr_all_0s_card_a,
       cri.max_mdbtovr_all_0s_pos_a,
       cri.max_mdbtovr_all_0s_cash_a,
       cri.max_mdbt_all_ever,
       cri.max_mdbt_all_ever_card,
       cri.max_mdbt_all_ever_pos,
       cri.max_mdbt_all_ever_cash,
       cri.max_mdbt_all_ever_a,
       cri.max_mdbt_all_ever_card_a,
       cri.max_mdbt_all_ever_pos_a,
       cri.max_mdbt_all_ever_cash_a,
       cri.max_mdbtovr_all_ever,
       cri.max_mdbtovr_all_ever_card,
       cri.max_mdbtovr_all_ever_pos,
       cri.max_mdbtovr_all_ever_cash,
       cri.max_mdbtovr_all_ever_a,
       cri.max_mdbtovr_all_ever_card_a,
       cri.max_mdbtovr_all_ever_pos_a,
       cri.max_mdbtovr_all_ever_cash_a,

       ovd.max_days3_0,
       ovd.max_days3_0_card,
       ovd.max_days3_0_pos,
       ovd.max_days3_0_cash,
       ovd.max_days3_0_cashtop,
       ovd.max_days3_0_cashtfa,
       ovd.max_days3_3,
       ovd.max_days3_3_card,
       ovd.max_days3_3_pos,
       ovd.max_days3_3_cash,
       ovd.max_days3_3_cashtop,
       ovd.max_days3_3_cashfa,
       ovd.max_days3_6,
       ovd.max_days3_6_card,
       ovd.max_days3_6_pos,
       ovd.max_days3_6_cash,
       ovd.max_days3_6_cashtop,
       ovd.max_days3_6_cashfa,
       ovd.max_days3_9,
       ovd.max_days3_9_card,
       ovd.max_days3_9_pos,
       ovd.max_days3_9_cash,
       ovd.max_days3_9_cashtop,
       ovd.max_days3_9_cashfa,
       ovd.max_days3_12,
       ovd.max_days3_12_card,
       ovd.max_days3_12_pos,
       ovd.max_days3_12_cash,
       ovd.max_days3_12_cashtop,
       ovd.max_days3_12_cashfa,
       ovd.max_days3_24,
       ovd.max_days3_24_card,
       ovd.max_days3_24_pos,
       ovd.max_days3_24_cash,
       ovd.max_days3_24_cashtop,
       ovd.max_days3_24_cashfa,
       ovd.max_days3_36,
       ovd.max_days3_36_card,
       ovd.max_days3_36_pos,
       ovd.max_days3_36_cash,
       ovd.max_days3_36_cashtop,
       ovd.max_days3_36_cashfa,
       ovd.max_days3_ever,
       ovd.min_days_to_ovrd,
       ovd.min_days_to_ovrd_card,
       ovd.min_days_to_ovrd_pos,
       ovd.min_days_to_ovrd_cash,
       ovd.min_days_to_ovrd_cashtop,
       ovd.min_days_to_ovrd_cashtfa,
       ovd.cnt_ovrd_3,
       ovd.cnt_ovrd_3_card,
       ovd.cnt_ovrd_3_pos,
       ovd.cnt_ovrd_3_cash,
       ovd.cnt_ovrd_3_cashtop,
       ovd.cnt_ovrd_3_cashfa,
       ovd.cnt_ovrd_6,
       ovd.cnt_ovrd_6_card,
       ovd.cnt_ovrd_6_pos,
       ovd.cnt_ovrd_6_cash,
       ovd.cnt_ovrd_6_cashtop,
       ovd.cnt_ovrd_6_cashfa,
       ovd.cnt_ovrd_9,
       ovd.cnt_ovrd_9_card,
       ovd.cnt_ovrd_9_pos,
       ovd.cnt_ovrd_9_cash,
       ovd.cnt_ovrd_9_cashtop,
       ovd.cnt_ovrd_9_cashfa,
       ovd.cnt_ovrd_12,
       ovd.cnt_ovrd_12_card,
       ovd.cnt_ovrd_12_pos,
       ovd.cnt_ovrd_12_cash,
       ovd.cnt_ovrd_12_cashtop,
       ovd.cnt_ovrd_12_cashfa,
       ovd.cnt_ovrd_24,
       ovd.cnt_ovrd_24_card,
       ovd.cnt_ovrd_24_pos,
       ovd.cnt_ovrd_24_cash,
       ovd.cnt_ovrd_24_cashtop,
       ovd.cnt_ovrd_24_cashfa,
       ovd.cnt_ovrd_36,
       ovd.cnt_ovrd_36_card,
       ovd.cnt_ovrd_36_pos,
       ovd.cnt_ovrd_36_cash,
       ovd.cnt_ovrd_36_cashtop,
       ovd.cnt_ovrd_36_cashfa,
       ovd.cnt_ovrd_ever,

       def.month_from_last_deferment_end,
       def.month_from_first_deferment_end,
       def.month_from_last_deferment_start,
       def.month_from_first_deferment_start,
       def.last_deferment_type
from step_0_masterlist                  ml
left
join step_3_contract_features           crt
      on crt.client_rbo_gid  = ml.client_rbo_gid
     and crt.application_day = ml.application_day
left
join step_4_turnovers_features          trn
      on trn.client_rbo_gid  = ml.client_rbo_gid
     and trn.application_day = ml.application_day
left
join step_6_credit_index_daily_features cri
      on cri.client_rbo_gid  = ml.client_rbo_gid
     and cri.application_day = ml.application_day
left
join step_7_overdue_index_features      ovd
      on ovd.client_rbo_gid  = ml.client_rbo_gid
     and ovd.application_day = ml.application_day
left
join step_10_deferments_features        def
      on def.client_rbo_gid  = ml.client_rbo_gid
     and def.application_day = ml.application_day

