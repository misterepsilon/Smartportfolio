/* =======================================================================
   TAQ 1-minute previous-tick par PERMNO — sortie unique et colonnes minimales
   ======================================================================= */
proc datasets lib=work kill nolist; quit;

options mprint mlogic symbolgen compress=yes;

/* ====== PARAMS À ADAPTER ====== */
%let FIRST_DATE = 20240101;      /* YYYYMMDD */
%let LAST_DATE  = 20250630;      /* YYYYMMDD */
%let PERMNO_TAB = mylib.my_stocks;  /* table contenant au moins PERMNO (distinct) */

/* Libnames (adapter TON_LOGIN / chemins WRDS) */
libname mylib  "/home/hecca/aadmberrada/extract_database/data";
libname outlib "/scratch/hecca/aadmb";

/* ====== Nom de la table finale ====== */
%let OUT_TBL_FINAL = HF_PRICES_&FIRST_DATE._&LAST_DATE;


/* ====== MAPPING PERMNO → SYM_ROOT/SYM_SUFFIX (TAQ) ====== */
proc sql;
  create table work.taq_permno_map as
  select distinct a.permno
       , b.sym_root
       , coalesce(b.sym_suffix,'') as sym_suffix length=8
  from (select distinct permno from &PERMNO_TAB) as a
  inner join WRDSAPPS.TAQMCLINK as b
    on a.permno = b.permno
  where not missing(b.sym_root)
  ;
quit;

proc sql noprint;
  select count(distinct permno) into :N_PERMNO trimmed from work.taq_permno_map;
  select count(*) into :N_SYMS trimmed from work.taq_permno_map;
quit;
%put NOTE: Univers PERMNO= &N_PERMNO ; %put NOTE: Paires (root,suffix)= &N_SYMS ;

/* ====== LISTE DES DATASETS JOURNALIERS TAQ (TAQMSEC.CTM_YYYYMMDD) ====== */
%macro build_ctm_list(first_yyyymmdd, last_yyyymmdd);
  data work._ctm_catalog;
    set sashelp.vtable(keep=libname memname);
    where upcase(libname)="TAQMSEC" and prxmatch('/^CTM_\d{8}$/', strip(memname))=1;
    date_ = input(substr(memname,5,8), yymmdd8.);
  run;

  %let FIRST_SAS = %sysfunc(inputn(&first_yyyymmdd, yymmdd8.));
  %let LAST_SAS  = %sysfunc(inputn(&last_yyyymmdd , yymmdd8.));

  proc sql;
    create table work.ctm_in_range as
    select cats(libname,'.',memname) as dsname length=32
         , date_
    from work._ctm_catalog
    where date_ between &FIRST_SAS and &LAST_SAS
    order by date_;
  quit;

  data _null_;
    set work.ctm_in_range end=last;
    call symputx(cats('CTMDS',_n_), dsname, 'G');
    call symputx(cats('CTMDATE',_n_), put(date_, yymmddn8.), 'G'); /* YYYYMMDD */
    if last then call symputx('N_DAYS', _n_, 'G');
  run;

  %put NOTE: Jours TAQ trouvés = &N_DAYS ;
%mend;
%build_ctm_list(&FIRST_DATE, &LAST_DATE);

/* ====== Option perf: sauter la médiane si date >= 01APR2015 (horodatage µs) ====== */
%let CUTOFF_MEDIAN = %sysfunc(inputn(20150401, yymmdd8.));

/* ====== Créer la table finale si elle n'existe pas, avec formats/longueurs fixes ====== */
%macro ensure_final_table;
  %if %sysfunc(exist(outlib.&OUT_TBL_FINAL)) = 0 %then %do;
    data outlib.&OUT_TBL_FINAL (compress=yes);
      length TICKER $24 PERMNO 8 PRICE 8;
      format DATE yymmddn8. TIME time8.;
      stop; /* table vide, structure seulement */
    run;
  %end;
%mend;
%ensure_final_table

/* ====== PIPELINE JOURNALIER (avec chrono) ====== */
%macro process_one_day(i);
  %let _t0 = %sysfunc(datetime());  /* start chrono */

  %let ctm_ds   = &&CTMDS&i;       /* ex: TAQMSEC.CTM_20180103 */
  %let day_ymd  = &&CTMDATE&i;     /* 20180103 */
  %let day_sas  = %sysfunc(inputn(&day_ymd, yymmdd8.));

  %put NOTE: ====== Traitement &day_ymd sur &ctm_ds ======;

  /* 1) Filtrage côté base + jointure symboles (colonnes minimales) */
  proc sql;
    create table work.trades_raw as
    select  t.date
          , t.time_m
          , t.sym_root
          , coalesce(t.sym_suffix,'') as sym_suffix length=8
          , t.price
    from &ctm_ds as t
    inner join work.taq_permno_map as u
      on t.sym_root = u.sym_root
     and coalesce(t.sym_suffix,'') = u.sym_suffix
    where t.time_m between '09:30:00't and '16:00:00't
      and t.price    ne 0
      and t.ex in ('N','P','T','Q','A','D')
      and t.tr_corr = '00'
      /* Exclure sale conditions avec lettre (BN-H-S). On garde par ex. neutres/E/F. */
      and t.tr_scond not like '%A%' and t.tr_scond not like '%B%'
      and t.tr_scond not like '%C%' and t.tr_scond not like '%D%'
      and t.tr_scond not like '%G%' and t.tr_scond not like '%H%'
      and t.tr_scond not like '%I%' and t.tr_scond not like '%J%'
      and t.tr_scond not like '%K%' and t.tr_scond not like '%L%'
      and t.tr_scond not like '%M%' and t.tr_scond not like '%N%'
      and t.tr_scond not like '%O%' and t.tr_scond not like '%P%'
      and t.tr_scond not like '%Q%' and t.tr_scond not like '%R%'
      and t.tr_scond not like '%S%' and t.tr_scond not like '%T%'
      and t.tr_scond not like '%U%' and t.tr_scond not like '%V%'
      and t.tr_scond not like '%W%' and t.tr_scond not like '%X%'
      and t.tr_scond not like '%Y%' and t.tr_scond not like '%Z%'
    ;
  quit;

  /* 2) Médiane par timestamp (utile surtout avant 2015-04-01) */
  %if &day_sas < &CUTOFF_MEDIAN %then %do;
    proc sql;
      create table work.trades_ts as
      select date, time_m, sym_root, sym_suffix,
             median(price) as price
      from work.trades_raw
      group by sym_root, sym_suffix, time_m
      ;
    quit;
  %end;
  %else %do;
    /* Post 2015-04: on saute l’étape médiane (collisions quasi nulles) */
    data work.trades_ts;
      set work.trades_raw(keep=date time_m sym_root sym_suffix price);
    run;
  %end;

  /* 3) Ajout sentinelle 16:00:01 pour forcer complétude grille 1-min */
  proc sort data=work.trades_ts;
    by sym_root sym_suffix time_m;
  run;

  data work.trades_adj;
    set work.trades_ts;
    by sym_root sym_suffix;
    output;
    if last.sym_root and last.sym_suffix then do;
      time_m  = '16:00:01't;
      output;
    end;
  run;

  /* 4) Grille 1-minute previous-tick, 09:30 → 16:00 (strict) */
  %let START_T = '09:30:00't;
  %let END_T   = '16:00:00't;

  proc sort data=work.trades_adj;
    by sym_root sym_suffix date time_m;
  run;

  data work.min1;
    set work.trades_adj(keep=sym_root sym_suffix date time_m price);
    by sym_root sym_suffix date time_m;
    retain itime_m iprice;
    format itime_m time8.;

    if first.sym_root or first.sym_suffix or first.date then do;
      itime_m = &START_T;
      iprice  = .;
    end;

    /* Mettre à jour le dernier prix observé */
    if not missing(price) then iprice = price;

    /* Émettre sur grille tant que le tick courant a dépassé la prochaine minute */
    do while (time_m >= itime_m);
      if itime_m <= &END_T then output;
      itime_m = itime_m + 60; /* +60s */
    end;

    keep date sym_root sym_suffix itime_m iprice;
  run;

/* 5) Joindre PERMNO et façonner la table finale (colonnes minimales et ordonnées) */
proc sql;
  /* Crée la table avec les bons attributs (numériques longueur 8) */
  create table work.min1_final
    ( DATE   num format=yymmddn8.
    , TIME   num format=time8.
    , PERMNO num
    , TICKER char(24)
    , PRICE  num
    );

  /* Insère les données dans l'ordre souhaité */
  insert into work.min1_final (DATE, TIME, PERMNO, TICKER, PRICE)
  select  m.date
        , m.itime_m
        , u.permno
        , cats(m.sym_root, m.sym_suffix)
        , m.iprice
  from work.min1 as m
  left join work.taq_permno_map as u
    on m.sym_root=u.sym_root and m.sym_suffix=u.sym_suffix
  /* l’ORDER BY est inutile pour l’append, donc omis ici */
  ;
quit;

  /* 6) Append vers la table finale (formats déjà fixés → aucun warning) */
  proc append base=outlib.&OUT_TBL_FINAL data=work.min1_final force; run;

  /* 7) Nettoyage intermédiaire */
  proc datasets lib=work nolist;
    delete trades_raw trades_ts trades_adj min1 min1_final;
  quit;

  /* 8) Chrono fin + log */
  %let _t1 = %sysfunc(datetime());
  %let _elapsed = %sysevalf(&_t1 - &_t0);
  %put NOTE: >>> Jour &day_ymd traité en %sysfunc(putn(&_elapsed, best12.)) secondes <<<;

%mend;

/* ====== BOUCLE GLOBALE (par jour) ====== */
%macro run_all;
  %do i=1 %to &N_DAYS;
    %process_one_day(&i);
  %end;

  /* Récap final */
  %put NOTE: Pipeline terminé. Table finale: OUTLIB.&OUT_TBL_FINAL ;
%mend;

%run_all;