# kick off services
> docker compose up -d

# day 1
delete a manual msg from the 2nd partition. But in prod env, none msg should be deleted.
Will add DLQ and schema registry to avoid delete bad msg.


# day 2
producer __init__.py and replay.py

## verifications 1, all units of the same key falls into the same partitions
## vf2, event_time increase as cycle inscreases
## vf3, check total count adds up to 20631 (8725+7894+4012)
```bash
╰─❯ docker exec -it redpanda-0 rpk topic consume sensor.raw -p 0 --offset start -n 5 -f '%k\n'
7
7
7
7
7
...
╰─❯ docker exec -it redpanda-0 rpk topic consume sensor.raw --offset start -n 3 \
  -f '%v\n' | python -c "import sys,json; [print(json.loads(l)['unit_number'], json.loads(l)['time_in_cycles'], json.loads(l)['event_time']) for l in sys.stdin]"
1 1 1785007501000
1 2 1785007502000
1 3 1785007503000
...
╰─❯ docker exec -it redpanda-0 rpk topic describe sensor.raw -p
PARTITION  LEADER  EPOCH  REPLICAS  LOG-START-OFFSET  HIGH-WATERMARK
0          0       1      [0]       0                 8725
1          0       1      [0]       0                 7894
2          0       1      [0]       0                 4012
```


# day 3
- chose Avro over protobuf as it works well with Spark
- replace Producer with SerializingProducer
- replace manual json dump as the SerializingProducer knows the table schema by using .avsc file


# day 4

## 坏数据拦截. add DLQ topic

1. parse 层抓格式坏("wrong" 转不成数),stage=parse；Avro 层抓类型不符(str 冒充 long),stage=serialize。两个 traceback 长得完全不同,前者是 ValueError,后者是 fastavro 的 ValueSerializationError。

2. 加了 DLQ 后同样的坏行只是被隔离,其余照常, o.w. the whole replay.py fails without these try catch blocks

3. DLQ 用 1 分区(坏数据量小、不需并行不需 keyed)、DLQ 走 JSON 明文(给人看的、不该套 Avro)、DLQ record 带 line_num/stage/ts 做诊断上下文。这些选择本身都是可讲的判断。

4. confluent-kafka 的 callback 和 on_delivery 是同义参数，SerializingProducer 文档用on_delivery,两个 producer 统一用 on_delivery


# day 5
Add chaos, late-rate, late-max-ms to control late events ratio and max delay time


# lessons
紊乱作用在副本上，原始不可变。Esp. for duplicated record, original and dup one are referencing the same object. do a deepcopy