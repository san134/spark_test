
# coding: utf-8

# In[1]:


get_ipython().run_line_magic('run', "-i '/data01/sndbx_coe/santosh/kerberos_auth.py'")


# In[2]:


authenticate_kerberos()


# In[3]:


enviroment_vars["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2/lib/spark2"
findspark.init()


# In[13]:


import pickle
from pyspark.sql import Row, SparkSession, HiveContext
from pyspark.sql import functions as F

from pyspark.sql.types import ArrayType, StringType, IntegerType, FloatType


# In[5]:


warehouse_location = 'Spark-warehouse'

spark= SparkSession.builder.appName("Spark_e130487SnazareAggTest")                            .config("spark.sql.warehouse.dir",warehouse_location)                            .enableHiveSupport().getOrCreate()


# In[6]:


from pyspark.sql import Window


# In[25]:



test = [Row(MemberSK='MPI123',ClmSK=123,StartDT=20180105,EndDT=20180109,indexDT=20180915,src=1,IP=1,OP=0,ED=1,PCP=1,ccsDiag=['234','352'],ccsProc=['452'],como=['CHF','Diab'],DispCD='01'),
        Row(MemberSK='MPI123',ClmSK=125,StartDT=20180107,EndDT=20180108,indexDT=20180915,src=1,IP=0,OP=0,ED=0,PCP=1,ccsDiag=['470'],ccsProc=[],como=['Renal'],DispCD='04'),
        Row(MemberSK='MPI123',ClmSK=126,StartDT=20180101,EndDT=20180109,indexDT=20180915,src=1,IP=1,OP=0,ED=0,PCP=0,ccsDiag=['470'],ccsProc=[],como=[],DispCD='03'),
        Row(MemberSK='MPI123',ClmSK=127,StartDT=20180223,EndDT=20180302,indexDT=20180915,src=1,IP=1,OP=0,ED=0,PCP=0,ccsDiag=['456'],ccsProc=[],como=['Depresion'],DispCD='04'),
        Row(MemberSK='MPI123',ClmSK=128,StartDT=20180227,EndDT=20180305,indexDT=20180915,src=1,IP=1,OP=0,ED=1,PCP=1,ccsDiag=['119'],ccsProc=[],como=[],DispCD='01'),
        Row(MemberSK='MPI123',ClmSK=120,StartDT=20180502,EndDT=20180505,indexDT=20180915,src=1,IP=0,OP=1,ED=0,PCP=0,ccsDiag=['341'],ccsProc=['452','476'],como=['CHF'],DispCD=None),
#MedRec
        Row(MemberSK='MPI123',ClmSK=389,StartDT=20180227,EndDT=20180305,indexDT=20180915,src=2,IP=1,OP=0,ED=0,PCP=0,ccsDiag=['341'],ccsProc=[],como=['CAD'],DispCD=None),
        Row(MemberSK='MPI123',ClmSK=786,StartDT=20180602,EndDT=20180615,indexDT=20180915,src=2,IP=0,OP=1,ED=0,PCP=0,ccsDiag=['341'],ccsProc=['452','476'],como=['CHF'],DispCD=None),
#ADT
        Row(MemberSK='MPI123',ClmSK=793,StartDT=20180227,EndDT=20180305,indexDT=20180915,src=3,IP=1,OP=1,ED=0,PCP=0,ccsDiag=[],ccsProc=[],como=[],DispCD=None),


        Row(MemberSK='MPI456',ClmSK=145,StartDT=20180305,EndDT=20180310,indexDT=20181003,src=1,IP=0,OP=1,ED=0,PCP=0,ccsDiag=['890'],ccsProc=[],como=[],DispCD=None),
        Row(MemberSK='MPI456',ClmSK=148,StartDT=20180315,EndDT=20180320,indexDT=20181003,src=1,IP=1,OP=0,ED=1,PCP=0,ccsDiag=['897','789'],ccsProc=['452'],como=['CAD','Htn'],DispCD='04'),
        Row(MemberSK='MPI456',ClmSK=149,StartDT=20180320,EndDT=20180322,indexDT=20181003,src=1,IP=1,OP=0,ED=0,PCP=1,ccsDiag=['897'],ccsProc=['452','162'],como=['Renal'],DispCD='01'),
#MedRec
        Row(MemberSK='MPI456',ClmSK=658,StartDT=20180821,EndDT=20180827,indexDT=20181003,src=2,IP=1,OP=0,ED=0,PCP=1,ccsDiag=['700','890'],ccsProc=['452','162'],como=['CHF'], DispCD='01'),
#ADT
        Row(MemberSK='MPI456',ClmSK=490,StartDT=20180821,EndDT=20180821,indexDT=20181003,src=3,IP=1,OP=0,ED=0,PCP=1,ccsDiag=[],ccsProc=['452'],como=[], DispCD=None),
        Row(MemberSK='MPI456',ClmSK=789,StartDT=20180827,EndDT=20180827,indexDT=20181003,src=3,IP=1,OP=0,ED=0,PCP=1,ccsDiag=[],ccsProc=['452'],como=[], DispCD=None),


        Row(MemberSK='MPI789',ClmSK=764,StartDT=20180408,EndDT=20180415,indexDT=20181206,src=1,IP=1,OP=0,ED=0,PCP=0,ccsDiag=['451'],ccsProc=[],como=['Obesity'],DispCD='01'),
        Row(MemberSK='MPI789',ClmSK=769,StartDT=20180401,EndDT=20180409,indexDT=20181206,src=1,IP=1,OP=0,ED=0,PCP=0,ccsDiag=['878','451'],ccsProc=['999'],como=['Obesity','CHF'],DispCD=None),
        Row(MemberSK='MPI789',ClmSK=778,StartDT=20181004,EndDT=20181004,indexDT=20181206,src=1,IP=0,OP=1,ED=0,PCP=1,ccsDiag=['658'],ccsProc=[],como=['Diab'],DispCD=None),
        Row(MemberSK='MPI789',ClmSK=785,StartDT=20180131,EndDT=20180209,indexDT=20181206,src=1,IP=1,OP=0,ED=1,PCP=0,ccsDiag=['100'],ccsProc=[],como=['Diab'],DispCD='01'),
#ADT
        Row(MemberSK='MPI789',ClmSK=570,StartDT=20181006,EndDT=20181006,indexDT=20181206,src=3,IP=1,OP=0,ED=0,PCP=0,ccsDiag=['689'],ccsProc=[],como=[],DispCD=None),

#PriorAuth
        Row(MemberSK='MPI789',ClmSK=878,StartDT=20181006,EndDT=20181009,indexDT=20181206,src=4,IP=1,OP=0,ED=0,PCP=0,ccsDiag=[],ccsProc=[],como=[],DispCD='01')
        ]


# In[26]:


test2=spark.createDataFrame(test).repartition(2)


# In[27]:


test2.rdd.getNumPartitions()


# In[28]:


wndw = Window.partitionBy('MemberSK').orderBy(['StartDT','EndDT'])


# In[29]:


test3=test2.withColumn("GroupID", F.when(F.col("StartDT").                                    between(F.lag(F.col("StartDT"), 1).over(wndw), F.lag(F.col("EndDT"), 1).over(wndw)),                                    None                                    ).otherwise(F.monotonically_increasing_id()))            .withColumn("GroupID", F.last(F.col("GroupID"), ignorenulls=True).over(wndw.rowsBetween(Window.unboundedPreceding, 0))).cache()


# In[30]:


test3.show(10)


# In[31]:


test4=test3.groupBy(['MemberSK','GroupID','src']).agg(F.min(F.col('StartDT')).alias('StartDT'),
                                                      F.max(F.col('EndDT')).alias('EndDT'),
                                                      F.max(F.col('indexDT')).alias('indexDT'),
                                                        F.max(F.col('IP')).alias('IP'),
                                                        F.max(F.col('OP')).alias('OP'),
                                                        F.max(F.col('ED')).alias('ED'),
                                                        F.max(F.col('PCP')).alias('PCP'),
                                                        F.collect_set(F.col('ccsDiag')).alias('ccsDiag'),
                                                        F.collect_set(F.col('ccsProc')).alias('ccsProc'),
                                                        F.collect_set(F.col('como')).alias('como'),
                                                        F.last(F.col('DispCD')).alias('DispCD')
                                        ).cache()


# In[32]:


test4.show(5)


# In[33]:


def flattenlist(rowlist):
    return list(set([i for ln in rowlist for i in ln]))

flattenudf=F.udf(flattenlist,returnType=ArrayType(StringType()))


# In[34]:


test5=test4.withColumn('ccsDiagFlat',flattenudf(F.col('ccsDiag')))    .withColumn('ccsProcFlat',flattenudf(F.col('ccsProc')))    .withColumn('comoFlat',flattenudf(F.col('como')))    .drop('ccsDiag','ccsProc','como').cache()


# In[35]:


test5.take(10)


# In[36]:


wndw2 = Window.partitionBy(['MemberSK','GroupID']).orderBy('src')


# In[37]:


test6=test5.withColumn('rn',F.row_number().over(wndw2)).filter('rn=1').drop('rn')


# In[38]:


test6.show()


# In[62]:


test6.withColumn('DaystoIndex',F.datediff(F.from_unixtime(F.unix_timestamp(F.col('indexDT').cast(StringType()),'yyyymmdd')),
                                          F.from_unixtime(F.unix_timestamp(F.col('StartDT').cast(StringType()),'yyyymmdd'))                                          
                                         )
                ).show()


# In[61]:


test6.withColumn('DaystoIndex',F.from_unixtime(F.unix_timestamp(F.col('indexDT').cast(StringType()),'yyyymmdd'))).show()

