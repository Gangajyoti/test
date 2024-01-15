from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("abc").getOrCreate()



list_data = [(122,200,'2023-10-29',2738,'2023-11-05 11:00:00 UTC'),
(123,200,'2023-10-30',3217,'2023-11-05 11:00:00 UTC'),
(124,200,'2023-10-31',4193,'2023-11-05 11:00:00 UTC'),
(125,200,'2023-11-01',2931,'2023-11-05 11:00:00 UTC'),
(126,200,'2023-11-02',2017,'2023-11-05 11:00:00 UTC'),
(127,200,'2023-11-03',1936,'2023-11-05 11:00:00 UTC'),
(128,200,'2023-11-04',2813,'2023-11-05 11:00:00 UTC'),
(135,200,'2023-10-30',3281,'2023-11-06 11:00:00 UTC'),
(136,200,'2023-10-31',5162,'2023-11-06 11:00:00 UTC'),
(137,200,'2023-11-01',2931,'2023-11-06 11:00:00 UTC'),
(138,200,'2023-11-02',2021,'2023-11-06 11:00:00 UTC'),
(139,200,'2023-11-03',2007,'2023-11-06 11:00:00 UTC'),
(140,200,'2023-11-04',2813,'2023-11-06 11:00:00 UTC'),
(141,200,'2023-11-05',2703,'2023-11-06 11:00:00 UTC'),
(129,300,'2023-10-29',3737,'2023-11-05 11:00:00 UTC'),
(130,300,'2023-10-30',4216,'2023-11-05 11:00:00 UTC'),
(131,300,'2023-10-31',5192,'2023-11-05 11:00:00 UTC'),
(132,300,'2023-11-01',3930,'2023-11-05 11:00:00 UTC'),
(133,300,'2023-11-03',2935,'2023-11-05 11:00:00 UTC'),
(134,300,'2023-11-04',5224,'2023-11-05 11:00:00 UTC'),
(142,300,'2023-10-30',4274,'2023-11-06 11:00:00 UTC'),
(143,300,'2023-10-31',5003,'2023-11-06 11:00:00 UTC'),
(144,300,'2023-11-01',3930,'2023-11-06 11:00:00 UTC'),
(145,300,'2023-11-03',3810,'2023-11-06 11:00:00 UTC'),
(146,300,'2023-11-05',3702,'2023-11-06 11:00:00 UTC')]

Columns = ['id','retailerId','redemptionDate','redemptionCount','createDateTime']
ref_data = spark.createDataFrame(data=list_data, schema = Columns)

d = [(200,'XYZ Store','2020-01-28 11:36:21'),
(300,'ABC Store','2022-05-12 14:27:01'),
(400,'QRS Store','2022-05-12 14:27:01')
]
cols = ['id','retailerName','createDateTime']
ret_data = spark.createDataFrame(data=d, schema = cols)

joined_data = ref_data.join(ret_data,ref_data.retailerId == ret_data.id,how= 'inner').select(ref_data['*'])


w = Window.partitionBy('retailerId','redemptionDate').orderBy(desc('createDateTime'))

rd = ref_data.withColumn('Rank',dense_rank().over(w))

dates = ('2023-10-30', '2023-11-05')

f_o = rd.filter('retailerId==300').filter('Rank == 1').filter(rd.redemptionDate.between(*dates))

date_range = [['2023-10-30'], ['2023-10-31'], ['2023-11-01'], ['2023-11-02'],
               ['2023-11-03'], ['2023-11-04'], ['2023-11-05']]

s = spark.createDataFrame(date_range,schema=['date'])

f = s.join(f_o,s.date==f_o.redemptionDate,'left')

final = f.na.fill(0).select(['date','redemptionCount'])

final.show()