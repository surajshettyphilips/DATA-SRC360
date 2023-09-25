
df_kpi = spark.sql(f"""
select distinct qmel.qmnum as Notification,qmel.QMTXT as Notification_Description,CAST(concat(substring(qmel.erdat,0,4),'-' , substring(qmel.erdat,5,2),'-',substring(qmel.erdat,7,2)) as date) as CreatedOn,qmel.qmdat Notification_Date,
mara.matkl as group,
qmel.matnr Material,
makt.maktx as Material_description,
mara.matkl as materialGroupCode,
kna1.kukla as customerHierarchy,
"NAM" as Zone,
T023T.wgbez materialGroup ,
qmel.objnr Object_number,
mara.mtart Material_Type,
jcds.stat as status,
qmel.priok as priority,--I0072

mara.PRDHA productHierarchy ,
t179t.vtext BUdetails,
case when ihpa.parvw='WE' then "Ship_To" WHEN ihpa.parvw='AG' then "Sold_To" end Category,kna1.kunnr as `SoldTo`,
tj.txt04 Object_status,
TO_TIMESTAMP(CONCAT(jcds.UDATE, jcds.UTIME), 'yyyyMMddHHmmss') AS Updtimestamp,
to_date(jcds.udate,'yyyyMMdd') as  UpdatedDate,jcds.utime,jcds.inact,
kna1.name1 AccountName,
kna1.land1,kna1.regio,TKUKt.Vtext as customername,
jcds.stat


 from prod_L1.prd.qmel qmel

left join prod_l1.prd.mara mara ON mara.matnr=qmel.matnr

left join prod_l1.prd.makt makt ON makt.matnr=qmel.matnr

left join prod_l1.prd.ihpa ihpa ON ihpa.objnr=qmel.objnr

left join prod_l1.prd.kna1 kna1 ON kna1.kunnr=ihpa.parnr

left join prod_l1.prd.jcds jcds ON jcds.objnr=qmel.objnr

left join prod_l1.prd.tj02t tj ON tj.istat=jcds.stat

left join prod_l1.prd.adrc adrc ON kna1.adrnr=adrc.addrnumber

LEFT JOIN prod_l1.prd.T023T T023T ON T023T.matkl=MARA.matkl

LEFT JOIN prod_l1.prd.t179t t179t ON t179t.prodh=substr(mara.prdha,0,8)
LEFT JOIN prod_L1.prd.TKUKt TKUKt ON TKUKt.kukla=kna1.kukla 

 where ihpa.parvw in ('AG') and makt.spras='E' and tj.spras='E' and  TKUKt.spras='E' and len(t179t.prodh)=8

 and left(qmel.erdat,4)>2020 ---Change Year filter
and jcds.inact<>'X'   

--and upper(qmel.priok) in ('A', 'B', 'F', 'G', 'J', 'P') ---Compliance 

  """)

display(df_kpi )
#df_kpi.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save("wasbs://eda@sboxphilipsstore02.blob.core.windows.net/SRC/CustomerKPi")
