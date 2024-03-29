import streamlit as st
from trino.dbapi import connect
from datetime import date
import datetime
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import date, timedelta
import pygwalker as pyg 
import streamlit.components.v1 as stc 


plt.rcParams['font.family'] = 'AppleGothic'

st.set_page_config(
    page_title="Partner Data Solution팀 솔루션 대시보드 ",
    page_icon=":bar_chart:",
    layout="wide",
)


st.title('Partner Data Solution팀 솔루션 대시보드')

start_date = st.date_input('시작 날짜', datetime.date(2024, 1, 1))
yesterday = date.today() - timedelta(days=1)
end_date = st.date_input('종료 날짜', yesterday)

if st.button('데이터 조회'):
    conn = connect(
        host='ashptrino001-gcp.nfra.io',
        port='8080',
        http_scheme='http',
        catalog='hive_c3s',
        schema='nimo__db_ec',
        user='admin',
    verify=False)
    
    cur = conn.cursor()
    
    query = f"""
    /* QueryEngine: TRINO */
    with cal as (
    Select base_ymd
    From nimo__db_ec.nc_cd_cldr_ymd
    Where base_ymd between '{start_date}' AND '{end_date}'
    ), 
    sol as (
    select dt, soln_id, soln_nm
    from fdp__db_ec_base.sp_ms_solution_hist
    where dt between '{start_date}' AND '{end_date}'
    --  and soln_id in (@code1)
    ),
    ref as (
    select dt, originaltransactionid, cast(date(round_start_ymdt) as varchar) as round_start_ymd, cast(date(round_end_ymdt) as varchar) as round_end_ymd, cast(totalamount as bigint) as totalamount
    from fdp__db_ec_base.sp_ms_solution_payment_log
    where dt >= '2022-11-01'
    and element_at(items[1], 'type') = 'REFUND'
    ),
    -- 1) 발생 매출
    bal as (
        Select dt as ymd, soln_id, Sum(Case When element_at(items[1], 'type') <> 'REFUND' then cast(totalamount as bigint) else -cast(totalamount as bigint) end) as bal_amt
        From fdp__db_ec_base.sp_ms_solution_payment_log
        Where dt between '{start_date}' AND '{end_date}'
        Group by dt, soln_id
    ),
    -- 2) 기간 매출
    gigan as (
    Select
        p.base_ymd AS ymd
        , p.soln_id
        ,sum(
                Case When op.originaltransactionid is null And p.base_ymd > op.round_end_ymd Then 0 Else p.sales_unit End-- 기간인식매출
            + Case When op.originaltransactionid is not null And p.base_ymd = to_char(p.round_end_ymdt,'yyyy-mm-dd') Then p.sales_etc Else 0 End -- 보정매출
            + Case When op.originaltransactionid is null And p.base_ymd = op.round_end_ymd Then ( Cast(p.totalamount AS real) - Cast(op.totalamount AS real) ) - (p.sales_unit * Cast(date_diff('day',date(op.round_start_ymd),date(op.round_end_ymd)) AS real)) Else 0 End -- 보정매출( (결제내역-취소내역) - (과거기간인식매출 취소) )
        ) AS sales_amt
    From
    (
        Select cal.base_ymd
            , p0.*
            , floor(Cast(p0.totalamount AS real) / Cast(date_diff('day',date(p0.round_start_ymdt),date(p0.round_end_ymdt))+1 AS real)) AS sales_unit -- 기간인식매출 1일 인식액
            , Cast(p0.totalamount AS real) - floor(Cast(p0.totalamount AS real) / Cast(date_diff('day',date(p0.round_start_ymdt),date(p0.round_end_ymdt))+1 AS real)) * Cast(date_diff('day',date(p0.round_start_ymdt),date(p0.round_end_ymdt))+1 AS real) AS sales_etc -- 보정매출
        From cal
        Left Outer Join fdp__db_ec_base.sp_ms_solution_payment_log p0
        On date(cal.base_ymd) between date(p0.round_start_ymdt) And date(p0.round_end_ymdt)
        where p0.dt >= '2022-11-01'
        and (element_at(p0.items[1], 'type') = 'PAYMENT' or (element_at(p0.items[1], 'type') = 'UPGRADE' and date(p0.round_start_ymdt) >= date(p0.paymentconfirmdate)))
        union all
        select cal.base_ymd
            , p0.*
            , floor(Cast(p0.totalamount AS real) / Cast(date_diff('day',date(p0.paymentconfirmdate),date(p0.round_end_ymdt))+1 AS real)) AS sales_unit -- 기간인식매출 1일 인식액
            , Cast(p0.totalamount AS real) - floor(Cast(p0.totalamount AS real) / Cast(date_diff('day',date(p0.paymentconfirmdate),date(p0.round_end_ymdt))+1 AS real)) * Cast(date_diff('day',date(p0.paymentconfirmdate),date(p0.round_end_ymdt))+1 AS real) AS sales_etc -- 보정매출
        From cal
        Left Outer Join fdp__db_ec_base.sp_ms_solution_payment_log p0
        On date(cal.base_ymd) between date(p0.paymentconfirmdate) And date(p0.round_end_ymdt) 
        where p0.dt >= '2022-11-01'
        and element_at(p0.items[1], 'type') = 'UPGRADE' and date(p0.round_start_ymdt) < date(p0.paymentconfirmdate)
    ) p
    Left outer Join ref op On p.transactionid = op.originaltransactionid --And p.dt <= op.dt
    Group By 1,2
    ),
    -- 3) 완료 매출
    cmpl as (
        Select
            coalesce(op.dt, cast(date(p.round_end_ymdt) as varchar)) AS ymd
            , p.soln_id
            ,sum(cast(p.totalamount AS bigint) - coalesce(cast(op.totalamount as bigint),0)) AS cmpl_amt
        from fdp__db_ec_base.sp_ms_solution_payment_log p
        Left outer Join ref op On p.transactionid = op.originaltransactionid 
        where element_at(p.items[1], 'type') <> 'REFUND'
        And p.dt >= '2022-11-01'
        Group BY 1,2
    )
    -- 4) 집계 part
    Select concat('{start_date}','~', '{end_date}') as "분석 기간"
    /* 날짜 기준 */
        , sol.dt as "기준일"
    --     , cal.base_ywk_nm as "기준주"
    --     , cal.base_ym     as "기준월"
    --     , cal.base_qtr    as "기준분기"
    --     , cal.base_y      as "기준연도"
    --    -- 날짜 기준 : 전체
    /* solution type */
        , sol.soln_nm as "솔루션명"
    /* KPI */
        , coalesce(round(sum(bal.bal_amt)/1.1,1),0) as "발생 매출"
        , coalesce(round(sum(gigan.sales_amt)/1.1,1),0) as "기간 매출"
        , coalesce(round(sum(cmpl.cmpl_amt)/1.1,1),0) as "완료 매출"
    From sol
    left join bal on sol.dt = bal.ymd and sol.soln_id = bal.soln_id
    left join gigan on sol.dt = gigan.ymd and sol.soln_id = gigan.soln_id
    left join cmpl on sol.dt = cmpl.ymd and sol.soln_id = cmpl.soln_id
    left join (
        select ymd.base_ymd as base_ymd
            , ymd.base_dayw as base_dayw
            , wk.base_strt_ymd as base_ywk_nm
            , concat(ymd.base_y,'-',ymd.base_m) as base_ym
            , concat(ymd.base_y, '-', ymd.base_qtr, 'Q') as base_qtr
            , ymd.base_y as base_y
        from nimo__db_ec.nc_cd_cldr_ymd ymd
        left join nimo__db_ec.nc_cd_cldr_wk wk
        on ymd.base_ywk = wk.base_ywk
    ) cal on sol.dt = cal.base_ymd
    group by concat('{start_date}','~', '{end_date}')
        , sol.dt 
    --     , cal.base_ywk_nm 
    --     , cal.base_ym     
    --     , cal.base_qtr   
    --     , cal.base_y      
    /* solution type */
        , sol.soln_nm
    order by "분석 기간"
        , "솔루션명"
        
        
    """
    
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cols = [column[0] for column in cur.description]
    df = pd.DataFrame.from_records(data=rows, columns=cols)
    df = df[df['솔루션명'].isin(['상품명마스터','상품진단 솔루션', 'API데이터솔루션(통계)', '거래 Quick 모니터링', '유입 Quick 모니터링', '트렌드 Quick 모니터링'])].sort_values(by=['솔루션명', '기준일'])
    KPI_df = pd.DataFrame({'솔루션명':['상품명마스터', '상품진단 솔루션', 'API데이터솔루션(통계)', '거래 Quick 모니터링', '유입 Quick 모니터링', '트렌드 Quick 모니터링'],'KPI':[220000000, 360000000, 310000000, 11000000, 15000000, 14000000]}) # 정확한 수치 업데이트 필요
    df_merge = pd.merge(left=df, right=KPI_df, how = "inner", on = "솔루션명")
    df_merge['발생 매출'] = pd.to_numeric(df_merge['발생 매출'], errors='coerce')
    df_merge['누적 매출'] = round(df_merge.groupby('솔루션명')['발생 매출'].cumsum())
    df_merge['달성률'] = round((df_merge['누적 매출'] / df_merge['KPI'])*100,1)
    df_merge = df_merge[['솔루션명', '기준일', '발생 매출', '기간 매출', '완료 매출', '누적 매출', '달성률']]


    df_merge_api = df_merge[df_merge['솔루션명'] == 'API데이터솔루션(통계)']
    df_merge_kq = df_merge[df_merge['솔루션명'] == '거래 Quick 모니터링']
    df_merge_nm = df_merge[df_merge['솔루션명'] == '상품명마스터']
    df_merge_pd = df_merge[df_merge['솔루션명'] == '상품진단 솔루션']
    df_merge_uq = df_merge[df_merge['솔루션명'] == '유입 Quick 모니터링']
    df_merge_tq = df_merge[df_merge['솔루션명'] == '트렌드 Quick 모니터링']


    # API 호출량 데이터 호출
    query = f"""
    select a.identifier as "식별번호", a.account_id as "계정", b.acnt_nm as "계정명", b.chnl_no as "채널번호", b.chnl_nm as "채널명", b.chnl_url "채널 URL", a.API_CNT as "호출수"
    from (
        select distinct identifier,  account_id, count(identifier) as API_CNT
        from fdp__db_ec_base.sp_ms_api_center_api_access_log
        where dt between '{start_date}' AND '{end_date}'
            and identifier not in ('lZSXhajTiKCCJ2LHtDiY1', '29EKieYFSYNlA6f5VJZ2tl', '6ZelpkkYETOvWuLh6stMj', '6eKwxY9Lp4MpYflxnrobGf')
            and (gateway_route_url like '%/v1/bizdata-stats%' or gateway_route_url like '%/v1/customer-data%' )
        group by 1,2
        ) a
        
    left join
        (
        select acnt_id, acnt_nm, chnl_no, chnl_nm, chnl_type_nm, chnl_url
        from bi__db_ec_mart.sp_nvs_seller_hist
        where dt = '2024-01-24'
            and chnl_stat_cd = '900'
            and chnl_type_nm = '스마트스토어'
        ) b
        on a.account_id = b.acnt_id	
    order by a.API_CNT desc
    """

    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cols = [column[0] for column in cur.description]
    df_api = pd.DataFrame.from_records(data=rows, columns=cols)


    # API 요금제 데이터 호출
    query = f"""
        select
            concat('{start_date}','~', '{end_date}') as "분석 기간"
            , sbc.p_ymd              as "기준일"
        , cd_solu.comm_cd_nm              as "커머스 솔루션"
            , sbc.sbc_fees_cd       as "구독 요금제"
            , count(distinct sbc.acnt_id)     as "판매자 수"
        
    from    nimo__db_ec.ns_ec_solu_sbc_md01     sbc
    left join (
                select ymd.base_ymd         as base_ymd
                    , wk.base_strt_ymd     as base_ywk_nm
                    , concat(ymd.base_y,'-',ymd.base_m) as base_ym
                    , concat(ymd.base_y, '-', ymd.base_qtr, 'Q') as base_qtr
                    , ymd.base_y           as base_y
                from nimo__db_ec.nc_cd_cldr_ymd ymd
                left join
                    nimo__db_ec.nc_cd_cldr_wk wk
                on   ymd.base_ywk = wk.base_ywk
        ) cal
    on      sbc.p_ymd = cal.base_ymd
    left join
            nimo__db_ec.nc_cd_comm_mtr          cd_stat
    on      sbc.sbc_stat_cd = cd_stat.comm_cd
    and     cd_stat.grp_id = 'sbc_stat_cd'
    left join
            nimo__db_ec.nc_cd_comm_mtr          cd_solu
    on      sbc.solu_id = cd_solu.comm_cd
    and     cd_solu.grp_id = 'solu_id'
    left join
            nimo__db_ec.ns_ec_prod_catg_mtr     ctg
    on      sbc.acnt_rep_prod_catgl_cd = ctg.prod_catg_cd
    and     ctg.catg_lv = 1
    left join
            nimo__db_ec.nc_cd_comm_mtr          cd_acnt_tp
    on      sbc.rep_acnt_tp_cd = cd_acnt_tp.comm_cd
    and     cd_acnt_tp.grp_id = 'rep_acnt_tp_cd'
    left join
            nimo__db_ec.nc_cd_comm_mtr          cd_acnt_biz
    on      sbc.rep_acnt_biz_tp_cd = cd_acnt_biz.comm_cd
    and     cd_acnt_biz.grp_id = 'rep_acnt_biz_tp_cd'
    left join
            (select
                    acnt_id, max(acnt_nm) as acnt_nm
            from   nimo__db_ec.ns_ec_chnl_info_mtr
            group by 1
            ) acnt
    on      sbc.acnt_id = acnt.acnt_id

    where   1 = 1
    and     (sbc.p_ymd between '{start_date}' and '{end_date}')
    and    sbc.solu_id in ('4HiHNUP9KNULMFDijHhZ5f')        
    and    sbc.sbc_stat_cd in ('SUBSCRIBING','ON_PAYMENT_FAIL','WAITING_UNSUBSCRIPTION','WAITING_SUBSCRIPTION','CANCEL_SUBSCRIPTION','ON_EXAMINATION','UNSUBSCRIBED')     --> required filter : 구독 상태
    and     case when sbc.bef_end_ymd is not null then 'Y' else 'N' end in ('Y','N')   --> optional filter : 솔루션 재구독 여부
    group by
            concat('{start_date}','~', '{end_date}')
            , sbc.p_ymd
        , cd_solu.comm_cd_nm              
            , sbc.sbc_fees_cd       
    order by
            concat('{start_date}','~', '{end_date}')
            , sbc.p_ymd
        , cd_solu.comm_cd_nm              
            , sbc.sbc_fees_cd       

    """

    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cols = [column[0] for column in cur.description]
    df_api_cnt = pd.DataFrame.from_records(data=rows, columns=cols)
    df_api_cnt = df_api_cnt[['기준일', '구독 요금제', '판매자 수']]

    




    st.markdown("<h3 style=''text-align: center;''> </h3>", unsafe_allow_html=True)    
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["전체", "API데이터솔루션(통계)", "거래 Quick 모니터링", '상품명마스터', '상품진단 솔루션', '유입 Quick 모니터링', '트렌드 Quick 모니터링'])

    #전체
    with tab1:
        dash_1 = st.container()
        with dash_1:
            st.markdown("<h3 style=''text-align: center;''>Summary</h3>", unsafe_allow_html=True)
            st.write("")

        dash_2 = st.container()
        with dash_2:
            total_sales = df_merge['발생 매출'].sum()
            yst_sales = df_merge[df_merge['기준일'] == str(end_date)]['발생 매출'].sum()
            kpi_goals = round(total_sales/930000000*100,2) 
            
            yesterday2 = end_date - timedelta(days=1)
            yst_sales2 = df_merge[df_merge['기준일'] == str(yesterday2)]['발생 매출'].sum()
            total_sales2 = df_merge[df_merge['기준일'] < str(end_date)]['발생 매출'].sum()
            kpi_goals2 = round(total_sales2/930000000*100,2)

            col1, col2, col3, col4 = st.columns(4)
            col1.metric(label="매출", value=f"{round(yst_sales):,}원", delta = f"{round(yst_sales-yst_sales2):,}원")            
            col2.metric(label="누적 매출", value=f"{round(total_sales):,}원", delta = f"{round(total_sales-total_sales2):,}원")            
            col3.metric(label="24년 팀 KPI", value="930,000,000원")            
            col4.metric(label="KPI 달성률", value= str(kpi_goals)+"%", delta = str(round(kpi_goals-kpi_goals2,2))+"%")
            
        st.markdown("<h3 style=''text-align: center;''> </h3>", unsafe_allow_html=True)
        st.markdown("일별 매출")
        st.bar_chart(data=df_merge, x='기준일', y='발생 매출', color="솔루션명", width=0, height=0, use_container_width=True)

        st.markdown("일별 누적 매출")
        st.bar_chart(data=df_merge, x='기준일', y='누적 매출', color="솔루션명", width=0, height=0, use_container_width=True)

        st.markdown("<h3 style=''text-align: center;''> </h3>", unsafe_allow_html=True)
        st.markdown("<h3 style=''text-align: center;''>Detail</h3>", unsafe_allow_html=True)
        st.dataframe(data=df_merge, width=None, height=None, use_container_width=True, hide_index=True, column_order=None, column_config=None)

        st.markdown("<h3 style=''text-align: center;''> </h3>", unsafe_allow_html=True)
        st.markdown("<h3 style=''text-align: center;''>Custom</h3>", unsafe_allow_html=True)
        df_pyg = df_merge.copy()
        df_pyg = df_pyg[['솔루션명', '기준일', '발생 매출', '달성률']]
        pyg_html = pyg.walk(df_pyg,return_html=True)
        stc.html(pyg_html,scrolling=True,height=1000)

        
    
    #API데이터솔루션
        
    with tab2:
        dash_1 = st.container()
        with dash_1:
            st.markdown("<h3 style=''text-align: center;''>Summary</h3>", unsafe_allow_html=True)
            st.write("")
        
        #API 매출 Card
        dash_2 = st.container()
        with dash_2:
            total_sales = df_merge_api['발생 매출'].sum()
            yst_sales = df_merge_api[df_merge_api['기준일'] == str(end_date)]['발생 매출'].sum()
            kpi_goals = round(total_sales/310000000*100,2)

            yesterday2 = end_date - timedelta(days=1)
            yst_sales2 = df_merge_api[df_merge_api['기준일'] == str(yesterday2)]['발생 매출'].sum()
            total_sales2 = df_merge_api[df_merge_api['기준일'] < str(end_date)]['발생 매출'].sum()
            kpi_goals2 = round(total_sales2/310000000*100,2)

            col1, col2, col3, col4 = st.columns(4)
            col1.metric(label="매출", value=f"{round(yst_sales):,}원", delta = f"{round(yst_sales-yst_sales2):,}원")            
            col2.metric(label="총 매출", value=f"{round(total_sales):,}원", delta = f"{round(total_sales-total_sales2):,}원")            
            col3.metric(label="24년 KPI", value="310,000,000원")            
            col4.metric(label="KPI 달성률", value= str(kpi_goals)+"%", delta = str(round(kpi_goals-kpi_goals2,2))+"%")

        #API 호출량 Card
        dash_3 = st.container()
        with dash_3:
            st.markdown("<h4 style=''text-align: center;''></h4>", unsafe_allow_html=True)
            identifier_cnt = len(df_api["계정"].unique())
            api_cnt = df_api['호출수'].sum()
            api_free_brand = df_api_cnt[df_api_cnt['기준일'] == str(end_date)][df_api_cnt['구독 요금제']=="FREE"]["판매자 수"].sum()
            api_paid_brand = df_api_cnt[df_api_cnt['기준일'] == str(end_date)][df_api_cnt['구독 요금제']=="PAID"]["판매자 수"].sum()

            yesterday2 = end_date - timedelta(days=1)
            yst_api_free_brand = df_api_cnt[df_api_cnt['기준일'] == str(yesterday2)][df_api_cnt['구독 요금제']=="FREE"]["판매자 수"].sum()
            yst_api_paid_brand = df_api_cnt[df_api_cnt['기준일'] == str(yesterday2)][df_api_cnt['구독 요금제']=="PAID"]["판매자 수"].sum()

    
            col1, col2, col3, col4 = st.columns(4)
            col1.metric(label="호출 브랜드", value=f"{round(identifier_cnt):,}개")            
            col2.metric(label="API 호출수", value=f"{round(api_cnt):,}")            
            col3.metric(label="유료 가입자 브랜드", value=f"{round(api_free_brand):,}개", delta = f"{round(api_free_brand-yst_api_free_brand):,}개")            
            col4.metric(label="무료 가입자 브랜드", value=f"{round(api_paid_brand):,}개", delta = f"{round(api_paid_brand-yst_api_paid_brand):,}개")
            
        
        st.markdown("<h3 style=''text-align: center;''> </h3>", unsafe_allow_html=True)
        st.markdown("일별 매출")
        st.bar_chart(data=df_merge_api, x='기준일', y='발생 매출', color=None, width=0, height=0, use_container_width=True)
        st.markdown("누적 매출")
        st.bar_chart(data=df_merge_api, x='기준일', y='누적 매출', color=None, width=0, height=0, use_container_width=True)
        st.markdown("KPI 달성률")
        st.line_chart(data=df_merge_api, x='기준일', y='달성률', color=None, width=0, height=0, use_container_width=True)
        
                
        st.markdown("<h3 style=''text-align: center;''> </h3>", unsafe_allow_html=True)
        st.markdown("<h3 style=''text-align: center;''>Detail</h3>", unsafe_allow_html=True)
        st.markdown("<h4 style=''text-align: center;''>매출 현황 상세</h4>", unsafe_allow_html=True)
        st.dataframe(data=df_merge_api, width=None, height=None, use_container_width=True, hide_index=True, column_order=None, column_config=None)
        st.markdown("<h4 style=''text-align: center;''>브랜드별 호출량 상세</h4>", unsafe_allow_html=True)
        st.dataframe(data=df_api, width=None, height=None, use_container_width=True, hide_index=True, column_order=None, column_config=None)
        st.markdown("<h4 style=''text-align: center;''>요금제별 구독현황 상세</h4>", unsafe_allow_html=True)
        st.dataframe(data=df_api_cnt, width=None, height=None, use_container_width=True, hide_index=True, column_order=None, column_config=None)
        
    
    
    #거래Quick모니터링
    with tab3:
        dash_1 = st.container()
        with dash_1:
            st.markdown("<h3 style=''text-align: center;''>요약</h3>", unsafe_allow_html=True)
            st.write("")

        dash_2 = st.container()
        with dash_2:
            total_sales = df_merge_kq['발생 매출'].sum()
            yst_sales = df_merge_kq[df_merge_kq['기준일'] == str(end_date)]['발생 매출'].sum()
            kpi_goals = round(total_sales/11000000*100,2)

            yesterday2 = end_date - timedelta(days=1)
            yst_sales2 = df_merge_kq[df_merge_kq['기준일'] == str(yesterday2)]['발생 매출'].sum()
            total_sales2 = df_merge_kq[df_merge_kq['기준일'] < str(end_date)]['발생 매출'].sum()
            kpi_goals2 = round(total_sales2/11000000*100,2)

            col1, col2, col3, col4 = st.columns(4)
            col1.metric(label="매출", value=f"{round(yst_sales):,}원", delta = f"{round(yst_sales-yst_sales2):,}원")            
            col2.metric(label="총 매출", value=f"{round(total_sales):,}원", delta = f"{round(total_sales-total_sales2):,}원")            
            col3.metric(label="24년 KPI", value="11,000,000원")            
            col4.metric(label="KPI 달성률", value= str(kpi_goals)+"%", delta = str(round(kpi_goals-kpi_goals2,2))+"%")

        st.markdown("일별 매출")
        st.bar_chart(data=df_merge_kq, x='기준일', y='발생 매출', color=None, width=0, height=0, use_container_width=True)

        st.markdown("누적 매출")
        st.bar_chart(data=df_merge_kq, x='기준일', y='누적 매출', color=None, width=0, height=0, use_container_width=True)

        st.markdown("KPI 달성률")
        st.line_chart(data=df_merge_kq, x='기준일', y='달성률', color=None, width=0, height=0, use_container_width=True)

        st.dataframe(data=df_merge_kq, width=None, height=None, use_container_width=True, hide_index=True, column_order=None, column_config=None)

    #상품명마스터
    with tab4:
        dash_1 = st.container()
        with dash_1:
            st.markdown("<h3 style=''text-align: center;''>요약</h3>", unsafe_allow_html=True)
            st.write("")

        dash_2 = st.container()
        with dash_2:
            total_sales = df_merge_nm['발생 매출'].sum()
            yst_sales = df_merge_nm[df_merge_nm['기준일'] == str(end_date)]['발생 매출'].sum()
            kpi_goals = round(total_sales/220000000*100,2)

            yesterday2 = end_date - timedelta(days=1)
            yst_sales2 = df_merge_nm[df_merge_nm['기준일'] == str(yesterday2)]['발생 매출'].sum()
            total_sales2 = df_merge_nm[df_merge_nm['기준일'] < str(end_date)]['발생 매출'].sum()
            kpi_goals2 = round(total_sales2/220000000*100,2)

            col1, col2, col3, col4 = st.columns(4)
            col1.metric(label="매출", value=f"{round(yst_sales):,}원", delta = f"{round(yst_sales-yst_sales2):,}원")            
            col2.metric(label="총 매출", value=f"{round(total_sales):,}원", delta = f"{round(total_sales-total_sales2):,}원")            
            col3.metric(label="24년 KPI", value="220,000,000원")            
            col4.metric(label="KPI 달성률", value= str(kpi_goals)+"%", delta = str(round(kpi_goals-kpi_goals2,2))+"%")

        st.markdown("일별 매출")
        st.bar_chart(data=df_merge_nm, x='기준일', y='발생 매출', color=None, width=0, height=0, use_container_width=True)

        st.markdown("누적 매출")
        st.bar_chart(data=df_merge_nm, x='기준일', y='누적 매출', color=None, width=0, height=0, use_container_width=True)

        st.markdown("KPI 달성률")
        st.line_chart(data=df_merge_nm, x='기준일', y='달성률', color=None, width=0, height=0, use_container_width=True)

        st.dataframe(data=df_merge_nm, width=None, height=None, use_container_width=True, hide_index=True, column_order=None, column_config=None)    

    #상품진단
    with tab5:
        dash_1 = st.container()
        with dash_1:
            st.markdown("<h3 style=''text-align: center;''>요약</h3>", unsafe_allow_html=True)
            st.write("")

        dash_2 = st.container()
        with dash_2:
            total_sales = df_merge_pd['발생 매출'].sum()
            yst_sales = df_merge_pd[df_merge_pd['기준일'] == str(end_date)]['발생 매출'].sum()
            kpi_goals = round(total_sales/360000000*100,2)

            yesterday2 = end_date - timedelta(days=1)
            yst_sales2 = df_merge_pd[df_merge_pd['기준일'] == str(yesterday2)]['발생 매출'].sum()
            total_sales2 = df_merge_pd[df_merge_pd['기준일'] < str(end_date)]['발생 매출'].sum()
            kpi_goals2 = round(total_sales2/360000000*100,2)


            col1, col2, col3, col4 = st.columns(4)
            col1.metric(label="매출", value=f"{round(yst_sales):,}원", delta = f"{round(yst_sales-yst_sales2):,}원")            
            col2.metric(label="총 매출", value=f"{round(total_sales):,}원", delta = f"{round(total_sales-total_sales2):,}원")            
            col3.metric(label="24년 KPI", value="360,000,000원")            
            col4.metric(label="KPI 달성률", value= str(kpi_goals)+"%", delta = str(round(kpi_goals-kpi_goals2,2))+"%")

        st.markdown("일별 매출")
        st.bar_chart(data=df_merge_pd, x='기준일', y='발생 매출', color=None, width=0, height=0, use_container_width=True)

        st.markdown("누적 매출")
        st.bar_chart(data=df_merge_pd, x='기준일', y='누적 매출', color=None, width=0, height=0, use_container_width=True)
    
        st.markdown("KPI 달성률")
        st.bar_chart(data=df_merge_pd, x='기준일', y='달성률', color=None, width=0, height=0, use_container_width=True)

        st.dataframe(data=df_merge_pd, width=None, height=None, use_container_width=True, hide_index=True, column_order=None, column_config=None)
    
    #유입
    with tab6:
        dash_1 = st.container()
        with dash_1:
            st.markdown("<h3 style=''text-align: center;''>요약</h3>", unsafe_allow_html=True)
            st.write("")

        dash_2 = st.container()
        with dash_2:
            total_sales = df_merge_uq['발생 매출'].sum()
            yst_sales = df_merge_uq[df_merge_uq['기준일'] == str(end_date)]['발생 매출'].sum()
            kpi_goals = round(total_sales/15000000*100,2)
            
            yesterday2 = end_date - timedelta(days=1)
            yst_sales2 = df_merge_uq[df_merge_uq['기준일'] == str(yesterday2)]['발생 매출'].sum()
            total_sales2 = df_merge_uq[df_merge_uq['기준일'] < str(end_date)]['발생 매출'].sum()
            kpi_goals2 = round(total_sales2/15000000*100,2)

            col1, col2, col3, col4 = st.columns(4)
            col1.metric(label="매출", value=f"{round(yst_sales):,}원", delta = f"{round(yst_sales-yst_sales2):,}원")            
            col2.metric(label="총 매출", value=f"{round(total_sales):,}원", delta = f"{round(total_sales-total_sales2):,}원")            
            col3.metric(label="24년 KPI", value="15,000,000원")            
            col4.metric(label="KPI 달성률", value= str(kpi_goals)+"%", delta = str(round(kpi_goals-kpi_goals2,2))+"%")

        st.markdown("일별 매출")
        st.bar_chart(data=df_merge_uq, x='기준일', y='발생 매출', color=None, width=0, height=0, use_container_width=True)

        st.markdown("누적 매출")
        st.bar_chart(data=df_merge_uq, x='기준일', y='누적 매출', color=None, width=0, height=0, use_container_width=True)

        st.markdown("KPI 달성률")
        st.line_chart(data=df_merge_uq, x='기준일', y='달성률', color=None, width=0, height=0, use_container_width=True)

        st.dataframe(data=df_merge_uq, width=None, height=None, use_container_width=True, hide_index=True, column_order=None, column_config=None)

    #트렌드
    with tab7:
        dash_1 = st.container()
        with dash_1:
            st.markdown("<h3 style=''text-align: center;''>요약</h3>", unsafe_allow_html=True)
            st.write("")

        dash_2 = st.container()
        with dash_2:
            total_sales = df_merge_tq['발생 매출'].sum()
            yst_sales = df_merge_tq[df_merge_tq['기준일'] == str(end_date)]['발생 매출'].sum()
            kpi_goals = round(total_sales/14000000*100,2)

            yesterday2 = end_date - timedelta(days=1)
            yst_sales2 = df_merge_tq[df_merge_tq['기준일'] == str(yesterday2)]['발생 매출'].sum()
            total_sales2 = df_merge_tq[df_merge_tq['기준일'] < str(end_date)]['발생 매출'].sum()
            kpi_goals2 = round(total_sales2/14000000*100,2)

            col1, col2, col3, col4 = st.columns(4)
            col1.metric(label="매출", value=f"{round(yst_sales):,}원", delta = f"{round(yst_sales-yst_sales2):,}원")            
            col2.metric(label="총 매출", value=f"{round(total_sales):,}원", delta = f"{round(total_sales-total_sales2):,}원")            
            col3.metric(label="24년 KPI", value="14,000,000원")            
            col4.metric(label="KPI 달성률", value= str(kpi_goals)+"%", delta = str(round(kpi_goals-kpi_goals2,2))+"%")

        st.markdown("일별 매출")
        st.bar_chart(data=df_merge_tq, x='기준일', y='발생 매출', color=None, width=0, height=0, use_container_width=True)

        st.markdown("누적 매출")
        st.bar_chart(data=df_merge_tq, x='기준일', y='누적 매출', color=None, width=0, height=0, use_container_width=True)

        st.markdown("KPI 달성률")
        st.line_chart(data=df_merge_tq, x='기준일', y='달성률', color=None, width=0, height=0, use_container_width=True)

        st.dataframe(data=df_merge_tq, width=None, height=None, use_container_width=True, hide_index=True, column_order=None, column_config=None)    
    

    
    

