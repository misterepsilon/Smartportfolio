/* =========================================================
   RESET (nettoyage WORK uniquement)
========================================================= */
proc datasets lib=work kill nolist; quit;

/* =========================================================
   0) Paramètres & librairies
========================================================= */
options mprint mlogic symbolgen compress=yes;
%let START_DATE = '01JAN1970'd;
%let END_DATE   = '30JUN2025'd;

libname outlib "/scratch/hecca/aadm";  /* TABLES FINALES (FULL) ICI */
libname mylib  "/home/hecca/aadmberrada/extract_database/data";  /* KRIS + CONSTITUENTS */

/* =========================================================
   1) Jours de bourse (CRSP.DSI)
========================================================= */
proc sql;
  create table work.TRADING_DAYS as
  select distinct DATE format=yymmdd10. as DATE
  from crsp.dsi
  where DATE between &START_DATE and &END_DATE
  order by DATE;
quit;

proc datasets lib=work nolist;
  modify TRADING_DAYS; index create DATE;
quit;

/* =========================================================
   2) Membres S&P500 par jour (DSP500LIST_V2) + dates d’entrée/sortie
========================================================= */
proc sql;
  create table work.SP500_MEMBERSHIP as
  select  t.DATE,
          m.PERMNO,
          m.MbrStartDt as MBR_STARTDT,
          m.MbrEndDt   as MBR_ENDDT
  from work.TRADING_DAYS as t
  inner join crsp.dsp500list_v2 as m
    on t.DATE between m.MbrStartDt and coalesce(m.MbrEndDt,'31DEC9999'd)
  order by t.DATE, m.PERMNO;
quit;

proc datasets lib=work nolist;
  modify SP500_MEMBERSHIP; index create DATE; index create PERMNO;
quit;

/* =========================================================
   3) Ticker/nom valables le jour J (CRSP.STOCKNAMES) + on transporte MBR_*
========================================================= */
proc sql;
  create table work.SP500_DAY_RAW as
  select  m.DATE,
          m.PERMNO,
          m.MBR_STARTDT,
          m.MBR_ENDDT,
          s.PERMCO,
          s.TICKER,
          s.COMNAM,
          s.CUSIP,
          s.SHRCD,
          s.SHRCLS,
          s.NAMEDT,
          coalesce(s.NAMEENDDT, '31DEC9999'd) as NAMEENDDT
  from work.SP500_MEMBERSHIP as m
  left join crsp.stocknames as s
    on  m.PERMNO = s.PERMNO
    and m.DATE   between s.NAMEDT and coalesce(s.NAMEENDDT,'31DEC9999'd)
    and (s.SHRCD in (10,11))
  order by m.DATE, m.PERMNO, s.NAMEDT desc, calculated NAMEENDDT desc;
quit;

proc sort data=work.SP500_DAY_RAW;
  by DATE PERMNO descending NAMEDT descending NAMEENDDT;
run;

/* 1 ligne par (DATE, PERMNO) */
data work.SP500_DAY;
  set work.SP500_DAY_RAW;
  by DATE PERMNO;
  if first.PERMNO then output;
run;

/* =========================================================
   4) KRIS : priorité PERMNO, fallback PERMCO
========================================================= */

/* 4.1 — Liens KRIS par PERMNO */
proc sql;
  create table work.KRIS_PERMNO as
  select  Historical_CRSP_PERMNO_Link_to_C as PERMNO,
          Global_Company_Key               as GVKEY,
          coalesce(First_Effective_Date_of_Link,'01JAN1900'd)  as FIRSTDT,
          coalesce(Last_Effective_Date_of_Link ,'31DEC9999'd)  as LASTDT
  from mylib.KRIS
  where not missing(Historical_CRSP_PERMNO_Link_to_C);
quit;
proc datasets lib=work nolist; modify KRIS_PERMNO; index create PERMNO; quit;

/* 4.2 — Liens KRIS par PERMCO (fallback) */
proc sql;
  create table work.KRIS_PERMCO as
  select  Historical_CRSP_PERMCO_Link_to_C as PERMCO,
          Global_Company_Key               as GVKEY,
          coalesce(First_Effective_Date_of_Link,'01JAN1900'd)  as FIRSTDT,
          coalesce(Last_Effective_Date_of_Link ,'31DEC9999'd)  as LASTDT
  from mylib.KRIS
  where not missing(Historical_CRSP_PERMCO_Link_to_C);
quit;
proc datasets lib=work nolist; modify KRIS_PERMCO; index create PERMCO; quit;

/* 4.3 — Candidats PERMNO (on transporte aussi MBR_*) */
proc sql;
  create table work.JOIN_PERMNO_ALL as
  select d.DATE, d.PERMNO, d.PERMCO,
         d.MBR_STARTDT, d.MBR_ENDDT,
         d.TICKER, d.COMNAM, d.CUSIP, d.SHRCD, d.SHRCLS,
         d.NAMEDT, d.NAMEENDDT,
         k.GVKEY, k.FIRSTDT as K_FIRSTDT, k.LASTDT as K_LASTDT
  from work.SP500_DAY as d
  left join work.KRIS_PERMNO as k
    on d.PERMNO = k.PERMNO
   and d.DATE   between k.FIRSTDT and k.LASTDT;
quit;

data work.JOIN_PERMNO_RANK;
  set work.JOIN_PERMNO_ALL;
  _HAS = (not missing(GVKEY));
run;
proc sort data=work.JOIN_PERMNO_RANK;
  by DATE PERMNO descending _HAS descending K_LASTDT descending K_FIRSTDT;
run;
data work.PERMNO_BEST;
  set work.JOIN_PERMNO_RANK;
  by DATE PERMNO;
  if first.PERMNO then output;
run;

/* 4.5 — Fallback via PERMCO si GVKEY manquant (on garde MBR_*) */
data work.NEED_PERMCO;
  set work.PERMNO_BEST;
  if missing(GVKEY);
  keep DATE PERMNO PERMCO MBR_STARTDT MBR_ENDDT TICKER COMNAM CUSIP SHRCD SHRCLS NAMEDT NAMEENDDT;
run;

proc sql;
  create table work.PERMCO_CAND as
  select n.*, kc.GVKEY as GVKEY_PERMCO, kc.FIRSTDT as P_FIRSTDT, kc.LASTDT as P_LASTDT
  from work.NEED_PERMCO as n
  left join work.KRIS_PERMCO as kc
    on n.PERMCO = kc.PERMCO
   and n.DATE   between kc.FIRSTDT and kc.LASTDT;
quit;

proc sort data=work.PERMCO_CAND;
  by DATE PERMNO descending P_LASTDT descending P_FIRSTDT;
run;

data work.PERMCO_BEST;
  set work.PERMCO_CAND;
  by DATE PERMNO;
  if first.PERMNO then output;
run;

/* 4.6 — Consolidation finale -> CONSTITUENTS (dans MYLIB) */
proc sql;
  create table mylib.SP500_CONSTITUENTS_DAILY(compress=binary) as
  select a.DATE,
         a.PERMNO,
         a.PERMCO,
         a.MBR_STARTDT, a.MBR_ENDDT,
         upcase(a.TICKER) as TICKER,
         upcase(a.COMNAM) as COMNAM,
         upcase(a.CUSIP)  as CUSIP,
         a.SHRCD, a.SHRCLS,
         a.NAMEDT, a.NAMEENDDT,
         coalescec(a.GVKEY, b.GVKEY_PERMCO) length=8 as GVKEY
  from work.PERMNO_BEST as a
  left join work.PERMCO_BEST as b
    on a.DATE=b.DATE and a.PERMNO=b.PERMNO
  order by a.DATE, a.PERMNO;
quit;

proc datasets lib=mylib nolist;
  modify SP500_CONSTITUENTS_DAILY;
  index create DATE;
  index create PERMNO;
  index create GVKEY;
quit;

/* =========================================================
   6) (Optionnel) Contrôles de base rapides
========================================================= */
proc sql;
  select count(*) as N_ROWS,
         count(distinct DATE) as N_DAYS,
         count(distinct PERMNO) as N_PERMNO
  from mylib.SP500_CONSTITUENTS_DAILY;
quit;

/* =========================================================
   --- ÉTAPE 3 : Prix / Retours / Liquidité (CRSP.DSF) ---
   Perf : on restreint DSF aux PERMNO de l’univers S&P500
========================================================= */
proc sql;
  create table work.UNIV_PERMNO as
  select distinct PERMNO
  from mylib.SP500_CONSTITUENTS_DAILY;
quit;

proc sql;
  create table work.DSF_CORE as
  select  d.DATE,
          d.PERMNO,
          d.PERMCO,
          /* DSF champs standard */
          d.PRC, d.RETX, d.RET, d.VOL, d.SHROUT, d.BIDLO, d.ASKHI, d.CFACPR, d.CFACSHR,
          /* DSF extras demandés */
          d.HEXCD, d.HSICCD, d.ISSUNO,
          d.CUSIP as CUSIP_HDR length=8,
          d.BID, d.ASK, d.OPENPRC, d.NUMTRD
  from crsp.DSF as d
  inner join work.UNIV_PERMNO as u
    on d.PERMNO = u.PERMNO
  where d.DATE between &START_DATE and &END_DATE
  ;
quit;

proc datasets lib=work nolist;
  modify DSF_CORE; index create DATE; index create PERMNO;
quit;

/* =========================================================
   --- ÉTAPE 4 : Distributions & Splits (CRSP.DSEDIST) ---
   Agrégation jour (PERMNO, EXDT)
========================================================= */
proc sql;
  create table work.DIST_DAILY_AGG as
  select  PERMNO,
          EXDT    as DATE format=yymmdd10.,
          sum(coalesce(DIVAMT,0)) as DIVAMT_SUM,
          /* produits multiplicatifs via somme des logs */
          exp(sum(case when FACPR  > 0 then log(FACPR)  else 0 end)) as FACPR_PROD,
          exp(sum(case when FACSHR > 0 then log(FACSHR) else 0 end)) as FACSHR_PROD,
          /* stats complémentaires */
          count(*) as DIST_EVENT_COUNT,
          min(DISTCD) as DISTCD_MIN,
          max(DISTCD) as DISTCD_MAX,
          min(PAYDT)  as PAYDT_MIN format=yymmdd10.,
          max(PAYDT)  as PAYDT_MAX format=yymmdd10.,
          min(RCRDDT) as RCRDDT_MIN format=yymmdd10.,
          max(RCRDDT) as RCRDDT_MAX format=yymmdd10.
  from crsp.DSEDIST
  where EXDT between &START_DATE and &END_DATE
  group by PERMNO, EXDT
  ;
quit;

proc datasets lib=work nolist;
  modify DIST_DAILY_AGG; index create DATE; index create PERMNO;
quit;

/* =========================================================
   --- ÉTAPE 5 : Delistings (CRSP.DSEDELIST)
========================================================= */
proc sql;
  create table work.DELIST_DAILY as
  select  PERMNO,
          DLSTDT  as DATE format=yymmdd10.,
          DLSTCD,
          DLPRC,
          DLRET
  from crsp.DSEDELIST
  where DLSTDT between &START_DATE and &END_DATE
  ;
quit;

proc sort data=work.DELIST_DAILY; by DATE PERMNO; run;

/* =========================================================
   --- ÉTAPE 6 : Indices de marché (CRSP.DSI) : core + extras ---
========================================================= */
proc sql;
  create table work.DSI_CORE as
  select  DATE,
          VWRETD, EWRETD,     /* avec dividendes */
          VWRETX, EWRETX,     /* sans dividendes */
          SPRTRN, SPINDX,
          TOTCNT, USDCNT, TOTVAL, USDVAL
  from crsp.DSI
  where DATE between &START_DATE and &END_DATE
  ;
quit;

proc datasets lib=work nolist;
  modify DSI_CORE; index create DATE;
quit;

/* =========================================================
   --- TABLE FINALE JOURNALIÈRE (dans OUTLIB)
========================================================= */
proc sql;
  create table outlib.SP500_DAILY_FULL(compress=binary) as
  select
      u.DATE,
      u.PERMNO, u.PERMCO, u.GVKEY,
      u.MBR_STARTDT, u.MBR_ENDDT,
      u.TICKER, u.COMNAM, u.CUSIP, u.SHRCD, u.SHRCLS, u.NAMEDT, u.NAMEENDDT,

      /* ---- Marché (DSF) ---- */
      d.PRC, d.RETX, d.RET, d.VOL, d.SHROUT, d.BIDLO, d.ASKHI, d.CFACPR, d.CFACSHR,

      /* Dérivés simples */
      case when d.PRC is not null and d.SHROUT is not null
           then abs(d.PRC) * d.SHROUT else . end as ME,
      case when d.VOL is not null and d.SHROUT > 0
           then d.VOL / d.SHROUT else . end as TURNOVER,
      case when d.PRC is not null and d.VOL is not null
           then abs(d.PRC) * d.VOL else . end as DOLLAR_VOL,

      /* Extras DSF */
      d.HEXCD, d.HSICCD, d.ISSUNO,
      d.CUSIP_HDR,
      d.BID, d.ASK, d.OPENPRC, d.NUMTRD,

      /* ---- Distributions / Splits (agrégés jour) ---- */
      g.DIVAMT_SUM, g.FACPR_PROD, g.FACSHR_PROD,
      g.DIST_EVENT_COUNT, g.DISTCD_MIN, g.DISTCD_MAX,
      g.PAYDT_MIN, g.PAYDT_MAX, g.RCRDDT_MIN, g.RCRDDT_MAX,

      /* ---- Delistings + Retour combiné ---- */
      dl.DLSTCD, dl.DLPRC, dl.DLRET,
      case when dl.DLRET is not null
             then (1 + coalesce(d.RET,0))*(1 + dl.DLRET) - 1
           else d.RET
      end as RET_COMBINED,

      /* ---- Indices CRSP (benchmarks) ---- */
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

/* Index (simples + composite) */
proc datasets lib=outlib nolist;
  modify SP500_DAILY_FULL;
  index create DATE;
  index create PERMNO;
  index create GVKEY;
  index create DATE_PERMNO=(DATE PERMNO);
quit;

/* =========================================================
   Contrôles rapides (qualité & cohérence)
========================================================= */
proc sql;
  /* couverture ret/dlret */
  select sum(RET is not null)   as N_RET,
         sum(DLRET is not null) as N_DLRET,
         sum(RET_COMBINED is not null) as N_RET_COMBINED
  from outlib.SP500_DAILY_FULL;

  /* 1 ligne / (date, permno) */
  select count(*) as N_ROWS,
         count(distinct catx('_',put(DATE,yymmdd10.),put(PERMNO,8.))) as N_KEYS
  from outlib.SP500_DAILY_FULL;

  /* jours sans trade (PRC & RET manquants) */
  select sum(PRC is missing and RET is missing) as N_MISSING_PRICE_DAYS
  from outlib.SP500_DAILY_FULL;

  /* remplissage de quelques nouveaux champs */
  select sum(HEXCD   is not null) as N_HEXCD_FILLED,
         sum(HSICCD  is not null) as N_HSICCD_FILLED,
         sum(BID     is not null) as N_BID_FILLED,
         sum(ASK     is not null) as N_ASK_FILLED,
         sum(OPENPRC is not null) as N_OPENPRC_FILLED,
         sum(NUMTRD  is not null) as N_NUMTRD_FILLED,
         sum(DIVAMT_SUM>0)        as N_DIV_DAYS,
         sum(VWRETX  is not null) as N_VWRETX_FILLED
  from outlib.SP500_DAILY_FULL;
quit;

/* =========================================================
   ===  VERSION MENSUELLE (dans OUTLIB) ====================
   Règles :
   - DATE = dernier jour de bourse du mois (EOM).
   - PRC/SHROUT/HEXCD/... = niveau du DERNIER JOUR du mois.
   - VOL/DOLLAR_VOL/NUMTRD/DIVAMT_SUM/... = SOMMES mensuelles.
   - RET/RETX/RET_COMBINED/VWRETD/... = rendements COMPOSÉS mensuels.
   - SPINDX = niveau du DERNIER JOUR du mois.
========================================================= */

/* 1) Ajouter la clé de fin de mois */
data work.DAILY_EOM;
  set outlib.SP500_DAILY_FULL;
  MDATE = intnx('month', DATE, 0, 'E');
  format MDATE yymmdd10.;
run;

/* 2) Dernier jour observé du mois par PERMNO */
proc sql;
  create table work.LAST_IN_MONTH as
  select PERMNO, MDATE, max(DATE) as LAST_DT format=yymmdd10.
  from work.DAILY_EOM
  group by PERMNO, MDATE;
quit;

/* 3) Niveaux du dernier jour du mois (identifiants & quotes de niveau) */
proc sql;
  create table work.LEVELS_LAST as
  select d.PERMNO, l.MDATE, l.LAST_DT,
         /* Identifiants au dernier jour du mois */
         d.PERMCO, d.GVKEY,
         d.MBR_STARTDT, d.MBR_ENDDT,
         d.TICKER, d.COMNAM, d.CUSIP, d.SHRCD, d.SHRCLS, d.NAMEDT, d.NAMEENDDT,
         /* Quotes/niveaux au dernier jour du mois */
         d.PRC, d.SHROUT, d.BIDLO, d.ASKHI, d.OPENPRC,
         d.HEXCD, d.HSICCD, d.ISSUNO, d.CUSIP_HDR,
         d.BID, d.ASK,
         d.DLSTCD, d.DLPRC, d.DLRET,
         d.SPINDX
  from work.DAILY_EOM d
  inner join work.LAST_IN_MONTH l
    on d.PERMNO=l.PERMNO and d.MDATE=l.MDATE and d.DATE=l.LAST_DT;
quit;

/* 4) Agrégations mensuelles par PERMNO */
proc sql;
  create table work.AGG_MONTH as
  select PERMNO, MDATE,
         /* Sommes mensuelles */
         sum(VOL)          as VOL,
         sum(DOLLAR_VOL)   as DOLLAR_VOL,
         sum(NUMTRD)       as NUMTRD,
         sum(DIVAMT_SUM)   as DIVAMT_SUM,
         sum(DIST_EVENT_COUNT) as DIST_EVENT_COUNT,
         min(DISTCD_MIN)   as DISTCD_MIN,
         max(DISTCD_MAX)   as DISTCD_MAX,
         min(PAYDT_MIN)    as PAYDT_MIN format=yymmdd10.,
         max(PAYDT_MAX)    as PAYDT_MAX format=yymmdd10.,
         min(RCRDDT_MIN)   as RCRDDT_MIN format=yymmdd10.,
         max(RCRDDT_MAX)   as RCRDDT_MAX format=yymmdd10.,
         /* Jours tradés dans le mois (au moins PRC ou RET non manquants) */
         sum( not (PRC is missing and RET is missing) ) as N_TRD_DAYS,
         /* Rendements composés sur les jours dispo du mois */
         (case when sum(RET  is not null)>0 then exp(sum(log(1+RET  ))) - 1 else . end) as RET,
         (case when sum(RETX is not null)>0 then exp(sum(log(1+RETX ))) - 1 else . end) as RETX,
         (case when sum(RET_COMBINED is not null)>0
               then exp(sum(log(1+RET_COMBINED))) - 1 else . end) as RET_COMBINED
  from work.DAILY_EOM
  group by PERMNO, MDATE;
quit;

/* 5) Benchmarks mensuels (une ligne par MDATE) */
proc sql;
  create table work.MARKET_MONTH as
  select MDATE,
         (case when sum(VWRETD is not null)>0 then exp(sum(log(1+VWRETD))) - 1 else . end) as VWRETD,
         (case when sum(EWRETD is not null)>0 then exp(sum(log(1+EWRETD))) - 1 else . end) as EWRETD,
         (case when sum(VWRETX is not null)>0 then exp(sum(log(1+VWRETX))) - 1 else . end) as VWRETX,
         (case when sum(EWRETX is not null)>0 then exp(sum(log(1+EWRETX))) - 1 else . end) as EWRETX,
         (case when sum(SPRTRN is not null)>0 then exp(sum(log(1+SPRTRN))) - 1 else . end) as SPRTRN,
         /* Comptages/valeurs agrégées marché (somme mensuelle) */
         sum(TOTCNT) as TOTCNT,
         sum(USDCNT) as USDCNT,
         sum(TOTVAL) as TOTVAL,
         sum(USDVAL) as USDVAL
  from work.DAILY_EOM
  group by MDATE;
quit;

/* 6) Table mensuelle finale (dans OUTLIB) */
proc sql;
  create table outlib.SP500_MONTHLY_FULL(compress=binary) as
  select
      l.MDATE as DATE format=yymmdd10.,
      l.PERMNO, l.PERMCO, l.GVKEY,
      l.MBR_STARTDT, l.MBR_ENDDT,
      l.TICKER, l.COMNAM, l.CUSIP, l.SHRCD, l.SHRCLS, l.NAMEDT, l.NAMEENDDT,

      /* Niveaux au dernier jour du mois */
      l.PRC, l.SHROUT, l.BIDLO, l.ASKHI, l.OPENPRC,
      l.HEXCD, l.HSICCD, l.ISSUNO, l.CUSIP_HDR, l.BID, l.ASK,
      l.DLSTCD, l.DLPRC, l.DLRET,
      l.SPINDX,

      /* Agrégations mensuelles (noms alignés sur la table daily) */
      a.RET, a.RETX, a.RET_COMBINED,
      a.VOL, a.DOLLAR_VOL, a.NUMTRD,
      a.DIVAMT_SUM,
      a.DIST_EVENT_COUNT, a.DISTCD_MIN, a.DISTCD_MAX,
      a.PAYDT_MIN, a.PAYDT_MAX, a.RCRDDT_MIN, a.RCRDDT_MAX,
      a.N_TRD_DAYS,

      /* Benchmarks mensuels (identiques pour toutes les lignes du mois) */
      m.VWRETD, m.EWRETD, m.VWRETX, m.EWRETX, m.SPRTRN,
      m.TOTCNT, m.USDCNT, m.TOTVAL, m.USDVAL

  from work.LEVELS_LAST  as l
  left join work.AGG_MONTH   as a on l.PERMNO = a.PERMNO and l.MDATE = a.MDATE
  left join work.MARKET_MONTH as m on l.MDATE  = m.MDATE
  order by DATE, PERMNO
  ;
quit;

/* Index utiles */
proc datasets lib=outlib nolist;
  modify SP500_MONTHLY_FULL;
  index create DATE;
  index create PERMNO;
  index create GVKEY;
  index create DATE_PERMNO=(DATE PERMNO);
quit;

/* 7) Sanity checks (mensuel) */
proc sql;
  select count(*) as N_ROWS, count(distinct DATE) as N_MONTHS
  from outlib.SP500_MONTHLY_FULL;

  /* Vérifier cohérence de composés vs sommes (simple spot check) */
  select DATE, sum(N_TRD_DAYS) as SUM_TRD_DAYS
  from outlib.SP500_MONTHLY_FULL
  group by DATE
  order by DATE desc;
quit;