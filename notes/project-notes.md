# train_FD001 raw
Top 3 rows in train_FD001.txt
unit, cycle, op_setting 1/2/3, sensor_1/2/.../21
```
1	1	-0.0007	-0.0004	100.0	518.67	641.82	1589.70	1400.60	14.62	21.61	554.36	2388.06	9046.19	1.3	47.47	521.66	2388.02	8138.62	8.4195	0.03	392	2388	100.0	39.06	23.4190	NaN	NaN
1	2	0.0019	-0.0003	100.0	518.67	642.15	1591.82	1403.14	14.62	21.61	553.75	2388.04	9044.07	1.3	47.49	522.28	2388.07	8131.49	8.4318	0.03	392	2388	100.0	39.00	23.4236	NaN	NaN
1	3	-0.0043	0.0003	100.0	518.67	642.35	1587.99	1404.20	14.62	21.61	554.26	2388.08	9052.94	1.3	47.27	522.42	2388.03	8133.23	8.4178	0.03	390	2388	100.0	38.95	23.3442	NaN	NaN
```

20631 rows altogether

Unique value count per column
col unit_number: 100 unique values
col time_in_cycles: 362 unique values
col op_setting_1: 158 unique values
col op_setting_2: 13 unique values
col op_setting_3: 1 unique values
col sensor_1: 1 unique values
col sensor_2: 310 unique values
col sensor_3: 3012 unique values
col sensor_4: 4051 unique values
col sensor_5: 1 unique values
col sensor_6: 2 unique values
col sensor_7: 513 unique values
col sensor_8: 53 unique values
col sensor_9: 6403 unique values
col sensor_10: 1 unique values
col sensor_11: 159 unique values
col sensor_12: 427 unique values
col sensor_13: 56 unique values
col sensor_14: 6078 unique values
col sensor_15: 1918 unique values
col sensor_16: 1 unique values
col sensor_17: 13 unique values
col sensor_18: 1 unique values
col sensor_19: 1 unique values
col sensor_20: 120 unique values
col sensor_21: 4745 unique values

# config docker-compose.yml
follow Redpanda docker compose labs recommendation and picked the one using single broker w/ console in docker

as I set console is `http://localhost:8080`, I can view the json msg queued to the topic

The topic is made with 3 partitions.

rpk topic consume 默认不提交 consumer group offset, 不会影响真正的consuemrs