/* =========================================================
   data_extraction_sp500_ciz.sas

   S&P 500 daily + monthly extraction using the CRSP CIZ format
   (extends through 2025+; legacy DSF capped at 2024-12-31).

   Differences vs data_extraction_sp500.sas (legacy):
     - Source tables (CIZ):
         crsp.dsf_v2            (replaces crsp.dsf,        date col = dlycaldt)
         crsp.stocknames_v2     (replaces crsp.stocknames, namedt/nameenddt kept)
         crsp.stkdistributions  (replaces crsp.dsedist,    ex-date  = disexdt)
         crsp.stkdelists        (replaces crsp.dsedelist,  date col = delistingdt)
     - crsp.dsi (legacy) is kept for the market benchmarks
       (VWRETD/EWRETD/SPRTRN/SPINDX/...). It caps at 2024-12-31 so
       these columns will be NULL for 2025+ rows in the output
       (graceful degradation).
     - "Common stock" filter: legacy shrcd in (10, 11) becomes
       securitytype='EQTY' AND securitysubtype='COM'
       AND sharetype IN ('NS', 'AD', 'SB', 'UG').
       Sharetype values: NS=normal stock, SB=subordinate (REITs like
       EQR/FRT/CPT/VNO), UG=unit group (dual-listed: CCL = Carnival
       Corp & plc), AD=ADR (rare in S&P 500, kept for fwd-compat).
       SHRCD column is synthesized (10 if usincflg='Y' else 11).
     - Categorical codes that were numeric in legacy are TEXT in CIZ:
         DISTCD (was integer; now disdetailtype, text codes)
         DLSTCD (was integer; now delreasontype, text codes)
         HEXCD  (was integer; now primaryexch, text like 'NYS','NAS')
       Output column names are kept (DISTCD/DLSTCD/HEXCD) but their
       dtype changes to character. Adjust downstream code accordingly.
     - ISSUNO is aliased from nasdissuno (NASDAQ issuer number).
       NULL for non-NASDAQ securities.
     - No KRIS / no GVKEY (same as legacy v6).

   Outputs (same names and structure as legacy):
     mylib.SP500_CONSTITUENTS_DAILY
     outlib.SP500_DAILY_FULL
     outlib.SP500_MONTHLY_FULL
========================================================= */

proc datasets lib=work kill nolist; quit;

options mprint mlogic symbolgen compress=yes;
%let START_DATE = '01JAN1970'd;
%let END_DATE   = '31DEC2025'd;

libname outlib "";
libname mylib  "";

/* =========================================================
   1) Trading days from CRSP DSF_V2 (CIZ; extends to 2025+).
      Legacy used crsp.dsi which caps at 2024-12-31.
========================================================= */
proc sql;
  create table work.TRADING_DAYS as
  select distinct dlycaldt as DATE format=yymmdd10.
  from crsp.dsf_v2
  where dlycaldt between &START_DATE and &END_DATE
  order by DATE;
quit;

proc datasets lib=work nolist;
  modify TRADING_DAYS; index create DATE;
quit;

/* =========================================================
   2) S&P 500 daily membership (DSP500LIST_V2 - same in CIZ).
========================================================= */
proc sql;
  create table work.SP500_MEMBERSHIP as
  select  t.DATE,
          m.permno as PERMNO,
          m.mbrstartdt as MBR_STARTDT,
          m.mbrenddt   as MBR_ENDDT
  from work.TRADING_DAYS as t
  inner join crsp.dsp500list_v2 as m
    on t.DATE between m.mbrstartdt and coalesce(m.mbrenddt, '31DEC9999'd)
  order by t.DATE, m.permno;
quit;

proc datasets lib=work nolist;
  modify SP500_MEMBERSHIP; index create DATE; index create PERMNO;
quit;

/* =========================================================
   3) Ticker / company name valid on day J (STOCKNAMES_V2).
      CIZ filter: EQTY/COM and sharetype in (NS, AD, SB, UG).
      Legacy aliases:
        issuernm    -> COMNAM
        shareclass  -> SHRCLS
        usincflg    -> SHRCD (10=Y, 11=N)
========================================================= */
proc sql;
  create table work.SP500_DAY_RAW as
  select  m.DATE,
          m.PERMNO,
          m.MBR_STARTDT,
          m.MBR_ENDDT,
          s.permco                  as PERMCO,
          s.ticker                  as TICKER,
          s.issuernm                as COMNAM,
          s.cusip                   as CUSIP,
          case when s.usincflg = 'Y' then 10
               when s.usincflg = 'N' then 11
               else .
          end                       as SHRCD,
          s.shareclass              as SHRCLS,
          s.namedt                  as NAMEDT,
          coalesce(s.nameenddt, '31DEC9999'd) as NAMEENDDT
  from work.SP500_MEMBERSHIP as m
  left join crsp.stocknames_v2 as s
    on  m.PERMNO = s.permno
    and m.DATE   between s.namedt and coalesce(s.nameenddt, '31DEC9999'd)
    and s.securitytype    = 'EQTY'
    and s.securitysubtype = 'COM'
    and s.sharetype in ('NS', 'AD', 'SB', 'UG')   /* NS=normal, SB=REITs, UG=dual-listed (CCL), AD=ADR */
  order by m.DATE, m.PERMNO, s.namedt desc, calculated NAMEENDDT desc;
quit;

proc sort data=work.SP500_DAY_RAW;
  by DATE PERMNO descending NAMEDT descending NAMEENDDT;
run;

/* 1 row per (DATE, PERMNO) */
data work.SP500_DAY;
  set work.SP500_DAY_RAW;
  by DATE PERMNO;
  if first.PERMNO then output;
run;

/* =========================================================
   4) Persist constituents (no GVKEY) -> MYLIB.
========================================================= */
proc sql;
  create table mylib.SP500_CONSTITUENTS_DAILY(compress=binary) as
  select  DATE,
          PERMNO,
          PERMCO,
          MBR_STARTDT, MBR_ENDDT,
          upcase(TICKER) as TICKER,
          upcase(COMNAM) as COMNAM,
          upcase(CUSIP)  as CUSIP,
          SHRCD, SHRCLS,
          NAMEDT, NAMEENDDT
  from work.SP500_DAY
  order by DATE, PERMNO;
quit;

proc datasets lib=mylib nolist;
  modify SP500_CONSTITUENTS_DAILY;
  index create DATE;
  index create PERMNO;
quit;

/* =========================================================
   5) Quick sanity counts.
========================================================= */
proc sql;
  select count(*) as N_ROWS,
         count(distinct DATE) as N_DAYS,
         count(distinct PERMNO) as N_PERMNO
  from mylib.SP500_CONSTITUENTS_DAILY;
quit;

/* =========================================================
   6) Daily prices / returns / liquidity (CRSP DSF_V2).
      Restricted to S&P 500 PERMNO universe.

      CIZ -> legacy column aliases:
        dlycaldt     -> DATE
        dlyprc       -> PRC
        dlyret       -> RET
        dlyretx      -> RETX
        dlyvol       -> VOL
        dlylow       -> BIDLO
        dlyhigh      -> ASKHI
        dlybid       -> BID
        dlyask       -> ASK
        dlyopen      -> OPENPRC
        dlynumtrd    -> NUMTRD
        dlycumfacpr  -> CFACPR
        dlycumfacshr -> CFACSHR
        primaryexch  -> HEXCD     (TEXT in CIZ; was INT in legacy)
        siccd        -> HSICCD
        nasdissuno   -> ISSUNO    (NASDAQ-specific; NULL for non-NASDAQ)
        cusip        -> CUSIP_HDR
========================================================= */
proc sql;
  create table work.UNIV_PERMNO as
  select distinct PERMNO
  from mylib.SP500_CONSTITUENTS_DAILY;
quit;

proc sql;
  create table work.DSF_CORE as
  select  d.dlycaldt     as DATE format=yymmdd10.,
          d.permno       as PERMNO,
          d.permco       as PERMCO,
          d.dlyprc       as PRC,
          d.dlyretx      as RETX,
          d.dlyret       as RET,
          d.dlyvol       as VOL,
          d.shrout       as SHROUT,
          d.dlylow       as BIDLO,
          d.dlyhigh      as ASKHI,
          d.dlycumfacpr  as CFACPR,
          d.dlycumfacshr as CFACSHR,
          d.primaryexch  as HEXCD length=8,
          d.siccd        as HSICCD,
          d.nasdissuno   as ISSUNO,
          d.cusip        as CUSIP_HDR length=8,
          d.dlybid       as BID,
          d.dlyask       as ASK,
          d.dlyopen      as OPENPRC,
          d.dlynumtrd    as NUMTRD
  from crsp.dsf_v2 as d
  inner join work.UNIV_PERMNO as u
    on d.permno = u.PERMNO
  where d.dlycaldt between &START_DATE and &END_DATE
  ;
quit;

proc datasets lib=work nolist;
  modify DSF_CORE; index create DATE; index create PERMNO;
quit;

/* =========================================================
   7) Distributions & splits (CRSP STKDISTRIBUTIONS).
      Aggregated to (PERMNO, DATE = disexdt).

      CIZ -> legacy aliases:
        disexdt        -> DATE
        disdivamt      -> DIVAMT      (-> DIVAMT_SUM after sum)
        disfacpr       -> FACPR       (-> FACPR_PROD after log-sum-exp)
        disfacshr      -> FACSHR      (-> FACSHR_PROD)
        disdetailtype  -> DISTCD      (TEXT in CIZ; alpha min/max preserved)
        dispaydt       -> PAYDT
        disrecorddt    -> RCRDDT
========================================================= */
proc sql;
  create table work.DIST_DAILY_AGG as
  select  permno  as PERMNO,
          disexdt as DATE format=yymmdd10.,
          sum(coalesce(disdivamt, 0)) as DIVAMT_SUM,
          exp(sum(case when disfacpr  > 0 then log(disfacpr)  else 0 end)) as FACPR_PROD,
          exp(sum(case when disfacshr > 0 then log(disfacshr) else 0 end)) as FACSHR_PROD,
          count(*) as DIST_EVENT_COUNT,
          min(disdetailtype) as DISTCD_MIN length=12,
          max(disdetailtype) as DISTCD_MAX length=12,
          min(dispaydt)      as PAYDT_MIN  format=yymmdd10.,
          max(dispaydt)      as PAYDT_MAX  format=yymmdd10.,
          min(disrecorddt)   as RCRDDT_MIN format=yymmdd10.,
          max(disrecorddt)   as RCRDDT_MAX format=yymmdd10.
  from crsp.stkdistributions
  where disexdt between &START_DATE and &END_DATE
  group by permno, disexdt
  ;
quit;

proc datasets lib=work nolist;
  modify DIST_DAILY_AGG; index create DATE; index create PERMNO;
quit;

/* =========================================================
   8) Delistings (CRSP STKDELISTS).

      CIZ -> legacy aliases:
        delistingdt    -> DATE
        delreasontype  -> DLSTCD (TEXT in CIZ; was INT in legacy)
        deldtprc       -> DLPRC
        delret         -> DLRET
========================================================= */
proc sql;
  create table work.DELIST_DAILY as
  select  permno         as PERMNO,
          delistingdt    as DATE format=yymmdd10.,
          delreasontype  as DLSTCD length=24,
          deldtprc       as DLPRC,
          delret         as DLRET
  from crsp.stkdelists
  where delistingdt between &START_DATE and &END_DATE
  ;
quit;

proc sort data=work.DELIST_DAILY; by DATE PERMNO; run;

/* =========================================================
   9) Market index (CRSP DSI - LEGACY).
      Caps at 2024-12-31; rows 2025+ get NULL benchmarks.
========================================================= */
proc sql;
  create table work.DSI_CORE as
  select  DATE,
          VWRETD, EWRETD,
          VWRETX, EWRETX,
          SPRTRN, SPINDX,
          TOTCNT, USDCNT, TOTVAL, USDVAL
  from crsp.dsi
  where DATE between &START_DATE and &END_DATE
  ;
quit;

proc datasets lib=work nolist;
  modify DSI_CORE; index create DATE;
quit;

/* =========================================================
   --- FINAL DAILY TABLE (in OUTLIB) ---
========================================================= */
proc sql;
  create table outlib.SP500_DAILY_FULL(compress=binary) as
  select
      u.DATE,
      u.PERMNO, u.PERMCO,
      u.MBR_STARTDT, u.MBR_ENDDT,
      u.TICKER, u.COMNAM, u.CUSIP, u.SHRCD, u.SHRCLS, u.NAMEDT, u.NAMEENDDT,

      /* ---- Market (DSF_V2) ---- */
      d.PRC, d.RETX, d.RET, d.VOL, d.SHROUT, d.BIDLO, d.ASKHI, d.CFACPR, d.CFACSHR,

      /* Simple derivatives */
      case when d.PRC is not null and d.SHROUT is not null
           then abs(d.PRC) * d.SHROUT else . end as ME,
      case when d.VOL is not null and d.SHROUT > 0
           then d.VOL / d.SHROUT else . end as TURNOVER,
      case when d.PRC is not null and d.VOL is not null
           then abs(d.PRC) * d.VOL else . end as DOLLAR_VOL,

      /* DSF extras */
      d.HEXCD, d.HSICCD, d.ISSUNO,
      d.CUSIP_HDR,
      d.BID, d.ASK, d.OPENPRC, d.NUMTRD,

      /* ---- Distributions / splits (daily aggregates) ---- */
      g.DIVAMT_SUM, g.FACPR_PROD, g.FACSHR_PROD,
      g.DIST_EVENT_COUNT, g.DISTCD_MIN, g.DISTCD_MAX,
      g.PAYDT_MIN, g.PAYDT_MAX, g.RCRDDT_MIN, g.RCRDDT_MAX,

      /* ---- Delistings + combined return ---- */
      dl.DLSTCD, dl.DLPRC, dl.DLRET,
      case when dl.DLRET is not null
             then (1 + coalesce(d.RET, 0)) * (1 + dl.DLRET) - 1
           else d.RET
      end as RET_COMBINED,

      /* ---- CRSP indices (benchmarks; NULL for dates 2025+) ---- */
      i.VWRETD, i.EWRETD, i.VWRETX, i.EWRETX, i.SPRTRN, i.SPINDX,
      i.TOTCNT, i.USDCNT, i.TOTVAL, i.USDVAL

  from mylib.SP500_CONSTITUENTS_DAILY as u
  left join work.DSF_CORE       as d  on u.PERMNO = d.PERMNO and u.DATE = d.DATE
  left join work.DIST_DAILY_AGG as g  on u.PERMNO = g.PERMNO and u.DATE = g.DATE
  left join work.DELIST_DAILY   as dl on u.PERMNO = dl.PERMNO and u.DATE = dl.DATE
  left join work.DSI_CORE       as i  on u.DATE   = i.DATE
  order by u.DATE, u.PERMNO
  ;
quit;

proc datasets lib=outlib nolist;
  modify SP500_DAILY_FULL;
  index create DATE;
  index create PERMNO;
  index create DATE_PERMNO=(DATE PERMNO);
quit;

/* =========================================================
   Quality / coherence checks (daily).
========================================================= */
proc sql;
  select sum(RET is not null)          as N_RET,
         sum(DLRET is not null)        as N_DLRET,
         sum(RET_COMBINED is not null) as N_RET_COMBINED
  from outlib.SP500_DAILY_FULL;

  select count(*) as N_ROWS,
         count(distinct catx('_', put(DATE, yymmdd10.), put(PERMNO, 8.))) as N_KEYS
  from outlib.SP500_DAILY_FULL;

  select sum(PRC is missing and RET is missing) as N_MISSING_PRICE_DAYS
  from outlib.SP500_DAILY_FULL;

  select sum(HEXCD   is not missing) as N_HEXCD_FILLED,
         sum(HSICCD  is not null)    as N_HSICCD_FILLED,
         sum(BID     is not null)    as N_BID_FILLED,
         sum(ASK     is not null)    as N_ASK_FILLED,
         sum(OPENPRC is not null)    as N_OPENPRC_FILLED,
         sum(NUMTRD  is not null)    as N_NUMTRD_FILLED,
         sum(DIVAMT_SUM > 0)         as N_DIV_DAYS,
         sum(VWRETX  is not null)    as N_VWRETX_FILLED  /* drops to 0 after 2024-12-31 */
  from outlib.SP500_DAILY_FULL;
quit;

/* =========================================================
   ===  MONTHLY VERSION (in OUTLIB) ========================
   Same rules as legacy:
     - DATE = last trading day of month (per PERMNO).
     - Levels (PRC, SHROUT, ...) = last day's value.
     - Flows (VOL, DOLLAR_VOL, NUMTRD, DIVAMT_SUM, ...) = monthly sum.
     - Returns (RET, RETX, RET_COMBINED, VWRETD, ...) = compounded
       via exp(sum(log(1+R))) - 1.
     - SPINDX = last day's level.
========================================================= */

/* 1) End-of-month key */
data work.DAILY_EOM;
  set outlib.SP500_DAILY_FULL;
  MDATE = intnx('month', DATE, 0, 'E');
  format MDATE yymmdd10.;
run;

/* 2) Last trading day of the month per PERMNO */
proc sql;
  create table work.LAST_IN_MONTH as
  select PERMNO, MDATE, max(DATE) as LAST_DT format=yymmdd10.
  from work.DAILY_EOM
  group by PERMNO, MDATE;
quit;

/* 3) Levels at the last trading day of the month */
proc sql;
  create table work.LEVELS_LAST as
  select d.PERMNO, l.MDATE, l.LAST_DT,
         d.PERMCO,
         d.MBR_STARTDT, d.MBR_ENDDT,
         d.TICKER, d.COMNAM, d.CUSIP, d.SHRCD, d.SHRCLS, d.NAMEDT, d.NAMEENDDT,
         d.PRC, d.SHROUT, d.BIDLO, d.ASKHI, d.OPENPRC,
         d.HEXCD, d.HSICCD, d.ISSUNO, d.CUSIP_HDR,
         d.BID, d.ASK,
         d.DLSTCD, d.DLPRC, d.DLRET,
         d.SPINDX
  from work.DAILY_EOM d
  inner join work.LAST_IN_MONTH l
    on d.PERMNO = l.PERMNO and d.MDATE = l.MDATE and d.DATE = l.LAST_DT;
quit;

/* 4) Monthly aggregations per PERMNO */
proc sql;
  create table work.AGG_MONTH as
  select PERMNO, MDATE,
         sum(VOL)              as VOL,
         sum(DOLLAR_VOL)       as DOLLAR_VOL,
         sum(NUMTRD)           as NUMTRD,
         sum(DIVAMT_SUM)       as DIVAMT_SUM,
         sum(DIST_EVENT_COUNT) as DIST_EVENT_COUNT,
         min(DISTCD_MIN)       as DISTCD_MIN length=12,
         max(DISTCD_MAX)       as DISTCD_MAX length=12,
         min(PAYDT_MIN)        as PAYDT_MIN  format=yymmdd10.,
         max(PAYDT_MAX)        as PAYDT_MAX  format=yymmdd10.,
         min(RCRDDT_MIN)       as RCRDDT_MIN format=yymmdd10.,
         max(RCRDDT_MAX)       as RCRDDT_MAX format=yymmdd10.,
         sum( not (PRC is missing and RET is missing) ) as N_TRD_DAYS,
         (case when sum(RET  is not null) > 0 then exp(sum(log(1 + RET )))  - 1 else . end) as RET,
         (case when sum(RETX is not null) > 0 then exp(sum(log(1 + RETX))) - 1 else . end) as RETX,
         (case when sum(RET_COMBINED is not null) > 0
               then exp(sum(log(1 + RET_COMBINED))) - 1 else . end) as RET_COMBINED
  from work.DAILY_EOM
  group by PERMNO, MDATE;
quit;

/* 5) Monthly benchmarks (one row per MDATE) */
proc sql;
  create table work.MARKET_MONTH as
  select MDATE,
         (case when sum(VWRETD is not null) > 0 then exp(sum(log(1 + VWRETD))) - 1 else . end) as VWRETD,
         (case when sum(EWRETD is not null) > 0 then exp(sum(log(1 + EWRETD))) - 1 else . end) as EWRETD,
         (case when sum(VWRETX is not null) > 0 then exp(sum(log(1 + VWRETX))) - 1 else . end) as VWRETX,
         (case when sum(EWRETX is not null) > 0 then exp(sum(log(1 + EWRETX))) - 1 else . end) as EWRETX,
         (case when sum(SPRTRN is not null) > 0 then exp(sum(log(1 + SPRTRN))) - 1 else . end) as SPRTRN,
         sum(TOTCNT) as TOTCNT,
         sum(USDCNT) as USDCNT,
         sum(TOTVAL) as TOTVAL,
         sum(USDVAL) as USDVAL
  from work.DAILY_EOM
  group by MDATE;
quit;

/* 6) Final monthly table (in OUTLIB) */
proc sql;
  create table outlib.SP500_MONTHLY_FULL(compress=binary) as
  select
      l.MDATE as DATE format=yymmdd10.,
      l.PERMNO, l.PERMCO,
      l.MBR_STARTDT, l.MBR_ENDDT,
      l.TICKER, l.COMNAM, l.CUSIP, l.SHRCD, l.SHRCLS, l.NAMEDT, l.NAMEENDDT,

      l.PRC, l.SHROUT, l.BIDLO, l.ASKHI, l.OPENPRC,
      l.HEXCD, l.HSICCD, l.ISSUNO, l.CUSIP_HDR, l.BID, l.ASK,
      l.DLSTCD, l.DLPRC, l.DLRET,
      l.SPINDX,

      a.RET, a.RETX, a.RET_COMBINED,
      a.VOL, a.DOLLAR_VOL, a.NUMTRD,
      a.DIVAMT_SUM,
      a.DIST_EVENT_COUNT, a.DISTCD_MIN, a.DISTCD_MAX,
      a.PAYDT_MIN, a.PAYDT_MAX, a.RCRDDT_MIN, a.RCRDDT_MAX,
      a.N_TRD_DAYS,

      m.VWRETD, m.EWRETD, m.VWRETX, m.EWRETX, m.SPRTRN,
      m.TOTCNT, m.USDCNT, m.TOTVAL, m.USDVAL

  from work.LEVELS_LAST   as l
  left join work.AGG_MONTH    as a on l.PERMNO = a.PERMNO and l.MDATE = a.MDATE
  left join work.MARKET_MONTH as m on l.MDATE  = m.MDATE
  order by DATE, PERMNO
  ;
quit;

proc datasets lib=outlib nolist;
  modify SP500_MONTHLY_FULL;
  index create DATE;
  index create PERMNO;
  index create DATE_PERMNO=(DATE PERMNO);
quit;

/* 7) Monthly sanity checks */
proc sql;
  select count(*) as N_ROWS, count(distinct DATE) as N_MONTHS
  from outlib.SP500_MONTHLY_FULL;

  select DATE, sum(N_TRD_DAYS) as SUM_TRD_DAYS
  from outlib.SP500_MONTHLY_FULL
  group by DATE
  order by DATE desc;
quit;
