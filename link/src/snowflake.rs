// 开始时间戳: 2023-05-24
const TWEPOCH: u128 = 1684922700000;
// 机器id所占的位数
const WORKER_ID_BITS: u128 = 5;
// 数据节点所占的位数
const DATA_CENTER_ID_BITS: u128 = 5;
// 支持最大的机器ID，最大是31
pub(crate) const MAX_WORKER_ID: u128 = (-1 ^ (-1 << WORKER_ID_BITS)) as u128;
// 支持的最大数据节点ID，结果是31
const MAX_DATA_CENTER_ID: u128 = (-1 ^ (-1 << DATA_CENTER_ID_BITS)) as u128;
// 序列号所占的位数
const SEQUENCE_BITS: u128 = 12;
// 工作节点标识ID向左移12位
const WORKER_ID_SHIFT: u128 = SEQUENCE_BITS;
// 数据节点标识ID向左移动17位: 12位序列号+5位工作节点
const DATA_CENTER_ID_SHIFT: u128 = SEQUENCE_BITS + WORKER_ID_BITS;
// 时间戳向左移动22位: 12位序列号+5位工作节点+5位数据节点
const TIMESTAMP_LEFT_SHIFT: u128 = SEQUENCE_BITS + WORKER_ID_BITS + DATA_CENTER_ID_BITS;
// 生成的序列掩码,这里是4095
const SEQUENCE_MASK: u128 = (-1 ^ (-1 << SEQUENCE_BITS)) as u128;

pub(crate) struct SnowflakeIdWorkerInner {
    worker_id: u128,
    data_center_id: u128,
    sequence: u128,
    last_timestamp: u128,
}

impl SnowflakeIdWorkerInner {
    pub(crate) fn new(
        worker_id: u128,
        data_center_id: u128,
    ) -> anyhow::Result<SnowflakeIdWorkerInner> {
        if worker_id > MAX_WORKER_ID {
            return Err(anyhow::anyhow!(
                "workerId:{} must be less than {}",
                worker_id,
                MAX_WORKER_ID
            ));
        }

        if data_center_id > MAX_DATA_CENTER_ID {
            return Err(anyhow::anyhow!(
                "datacenterId:{} must be less than {}",
                data_center_id,
                MAX_DATA_CENTER_ID
            ));
        }

        Ok(SnowflakeIdWorkerInner {
            worker_id,
            data_center_id,
            sequence: 0,
            last_timestamp: 0,
        })
    }

    pub(crate) fn next_id(&mut self) -> anyhow::Result<u64> {
        let mut timestamp = Self::get_time()?;
        if timestamp < self.last_timestamp {
            return Err(anyhow::anyhow!(
                "Clock moved backwards.  Refusing to generate id for {} milliseconds",
                self.last_timestamp - timestamp
            ));
        }

        if timestamp == self.last_timestamp {
            self.sequence = (self.sequence + 1) & SEQUENCE_MASK;
            if self.sequence == 0 {
                timestamp = Self::til_next_mills(self.last_timestamp)?;
            }
        } else {
            self.sequence = 0;
        }

        self.last_timestamp = timestamp;

        Ok((((timestamp - TWEPOCH) << TIMESTAMP_LEFT_SHIFT)
            | (self.data_center_id << DATA_CENTER_ID_SHIFT)
            | (self.worker_id << WORKER_ID_SHIFT)
            | self.sequence)
            .try_into()
            .unwrap())
    }

    fn til_next_mills(last_timestamp: u128) -> anyhow::Result<u128> {
        let mut timestamp = Self::get_time()?;
        while timestamp <= last_timestamp {
            timestamp = Self::get_time()?;
        }
        Ok(timestamp)
    }

    fn get_time() -> anyhow::Result<u128> {
        match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            Ok(s) => Ok(s.as_millis()),
            Err(_) => Err(anyhow::anyhow!("get_time error!")),
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn gen_id() {
        let mut id_worker = crate::snowflake::SnowflakeIdWorkerInner::new(1, 1).unwrap();
        let id1 = id_worker.next_id().unwrap();
        let id2 = id_worker.next_id().unwrap();
        println!("id1: {id1}");
        println!("id2: {id2}");
        assert!(id1 < id2)
    }
}
