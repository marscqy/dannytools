package myredis

import (
	"dannytools/constvar"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"dannytools/ehand"

	"github.com/go-redis/redis"
	//"github.com/davecgh/go-spew/spew"
)

var (
	G_Redis_BigKey_Format_Data      string         = "%-2d, %-10d, %-10d, %-6s, %-19s, %-10d, %-20s, %-20s\n"
	G_Redis_BigKey_Format_Header    string         = "%-2s, %-10s, %-10s, %-6s, %-19s, %-10s, %-20s, %-20s\n"
	G_Redis_BigKey_Headers          []string       = []string{"db", "sizeInByte", "elementCnt", "type", "expire", "bigSize", "key", "big"}
	G_regexp_info_keyspace_db       *regexp.Regexp = regexp.MustCompile(`db\d+`)
	G_regexp_info_replication_slave *regexp.Regexp = regexp.MustCompile(`slave\d+`)
)

func GetRedisBigkeyHeader() string {
	return fmt.Sprintf(G_Redis_BigKey_Format_Header, "db", "sizeInByte", "elementCnt", "type", "expire", "bigSize", "key", "big")
}

type RedisKey struct {
	Database        uint
	Bytes           int // bytes of key and value
	ElementCnt      int // for list|set|hash|zset, the count of the elements
	Type            string
	Expire          string
	MaxElementBytes int    // bytes of max element of key
	Key             string // key
	MaxElement      string // max element of key
	Value           interface{}
}

func (this RedisKey) GetRedisKeyPrintLine(ifBigkey bool, ifPretty bool, indent string) (string, error) {
	if ifBigkey {
		return this.ToStringWithoutValue(), nil
	} else {
		return this.DumpJsonStr(ifPretty, indent)
	}
}

func (this RedisKey) ToStringWithoutValue() string {
	var (
		key        string = this.Key
		maxElement string = this.MaxElement
	)
	if strings.Contains(this.Key, ",") {
		key = fmt.Sprintf("'%s'", this.Key)
	}
	if strings.Contains(this.MaxElement, ",") {
		maxElement = fmt.Sprintf("'%s'", this.MaxElement)
	}

	return fmt.Sprintf(G_Redis_BigKey_Format_Data, this.Database, this.Bytes, this.ElementCnt, this.Type, this.Expire, this.MaxElementBytes, key, maxElement)
}

func (this RedisKey) DumpJsonStr(ifPretty bool, indent string) (string, error) {
	var (
		err error
		//result  string
		reBytes []byte
	)

	if ifPretty {
		reBytes, err = json.MarshalIndent(this, "", indent)
	} else {
		reBytes, err = json.Marshal(this)
	}
	if err != nil {
		return "", ehand.WithStackError(err)
	} else {
		return string(reBytes), nil
	}

}

type ClusterAndRedisClient struct {
	Redis     *redis.Client
	Cluster   *redis.ClusterClient
	IsCluster bool
}

type ClusterAndRedisConf struct {
	Redis     *redis.Options
	Cluster   *redis.ClusterOptions
	IsCluster bool
}

type RedisAddr struct {
	Host string
	Port int
}

//slave0:ip=10.199.203.210,port=6683,state=online,offset=2047641,lag=1
type SlaveInfo struct {
	Addr        RedisAddr
	Replicating bool // state=online
	Lag         int
	Offset      int
}

type RedisRoleInfo struct {
	Role        string // master or slave
	MasterAddr  RedisAddr
	Replicating bool        // for slave, master_link_status:up
	Slaves      []SlaveInfo // for master
}

type ConfCommon struct {
	Password   string
	MaxRetries int

	DialTimeout  int
	ReadTimeout  int
	WriteTimeout int

	PoolSize           int
	PoolTimeout        int
	IdleTimeout        int
	IdleCheckFrequency int
}

func (this ConfCommon) setConfCommonRedis(opt *redis.Options) {

	opt.Password = this.Password

	if this.MaxRetries == 0 {
		opt.MaxRetries = 2
	} else {
		opt.MaxRetries = this.MaxRetries
	}

	if this.DialTimeout == 0 {
		opt.DialTimeout = 3 * time.Second
	} else {
		opt.DialTimeout = time.Duration(this.DialTimeout) * time.Second
	}

	if this.ReadTimeout == 0 {
		opt.ReadTimeout = 5 * time.Second
	} else {
		opt.ReadTimeout = time.Duration(this.ReadTimeout) * time.Second
	}

	if this.WriteTimeout == 0 {
		opt.WriteTimeout = 8 * time.Second
	} else {
		opt.WriteTimeout = time.Duration(this.WriteTimeout) * time.Second
	}

	if this.IdleCheckFrequency == 0 {
		opt.IdleCheckFrequency = 60 * time.Second
	} else {
		opt.IdleCheckFrequency = time.Duration(this.IdleCheckFrequency) * time.Second
	}

	if this.IdleTimeout == 0 {
		opt.IdleTimeout = 180 * time.Second
	} else {
		opt.IdleTimeout = time.Duration(this.IdleTimeout) * time.Second
	}

	if this.PoolTimeout == 0 {
		opt.PoolTimeout = 5 * time.Second
	} else {
		opt.PoolTimeout = time.Duration(this.PoolTimeout) * time.Second
	}

	if this.PoolSize == 0 {
		opt.PoolSize = 128
	} else {
		opt.PoolSize = this.PoolSize
	}

}

func (this ConfCommon) setConfCommonCluster(opt *redis.ClusterOptions) {

	opt.Password = this.Password

	if this.MaxRetries == 0 {
		opt.MaxRetries = 2
	} else {
		opt.MaxRetries = this.MaxRetries
	}

	if this.DialTimeout == 0 {
		opt.DialTimeout = 3 * time.Second
	} else {
		opt.DialTimeout = time.Duration(this.DialTimeout) * time.Second
	}

	if this.ReadTimeout == 0 {
		opt.ReadTimeout = 5 * time.Second
	} else {
		opt.ReadTimeout = time.Duration(this.ReadTimeout) * time.Second
	}

	if this.WriteTimeout == 0 {
		opt.WriteTimeout = 8 * time.Second
	} else {
		opt.WriteTimeout = time.Duration(this.WriteTimeout) * time.Second
	}

	if this.IdleCheckFrequency == 0 {
		opt.IdleCheckFrequency = 60 * time.Second
	} else {
		opt.IdleCheckFrequency = time.Duration(this.IdleCheckFrequency) * time.Second
	}

	if this.IdleTimeout == 0 {
		opt.IdleTimeout = 180 * time.Second
	} else {
		opt.IdleTimeout = time.Duration(this.IdleTimeout) * time.Second
	}

	if this.PoolTimeout == 0 {
		opt.PoolTimeout = 5 * time.Second
	} else {
		opt.PoolTimeout = time.Duration(this.PoolTimeout) * time.Second
	}

	if this.PoolSize == 0 {
		opt.PoolSize = 128
	} else {
		opt.PoolSize = this.PoolSize
	}

}

func GetClusterOrRedisClient(cfg ConfCommon, addrs []string, readOnly bool, db int, isCluster bool) (ClusterAndRedisClient, error) {
	var (
		client ClusterAndRedisClient
		err    error
	)
	client.IsCluster = isCluster
	if isCluster {
		cfgCluster := ConfCluster{ConfCommon: cfg, Addrs: addrs, ReadOnly: readOnly}
		client.Cluster, err = cfgCluster.CreateNewClientCluster()
	} else {
		cfgRedis := ConfRedis{ConfCommon: cfg, Addr: addrs[0], Database: db}
		client.Redis, err = cfgRedis.CreateNewClientRedis()
	}
	return client, err
}

func (client ClusterAndRedisClient) GetKeyType(key string) (string, error) {
	if client.IsCluster {
		return client.Cluster.Type(key).Result()
	} else {
		return client.Redis.Type(key).Result()
	}

}

/*
delete key by setting expire time of key to little seconds.
*/
func (client ClusterAndRedisClient) DeleteKeysByExpireIt(keyes []string, expTime time.Duration) error {
	var (
		err error
	)

	for _, key := range keyes {
		if client.IsCluster {
			_, err = client.Cluster.Expire(key, expTime).Result()
		} else {
			_, err = client.Redis.Expire(key, expTime).Result()
		}
		if err != nil {
			return ehand.WithStackError(err)
		}
	}
	return nil
}

func (client ClusterAndRedisClient) DeleteKeysByDirectlyDelIt(keyes []string) error {
	var (
		err error
	)

	if client.IsCluster {
		_, err = client.Cluster.Del(keyes...).Result()
	} else {
		_, err = client.Redis.Del(keyes...).Result()
	}
	if err != nil {
		return ehand.WithStackError(err)
	} else {
		return nil
	}

}

func (client ClusterAndRedisClient) GetKeyTTL(key string, timeFmt string) (string, error) {
	var (
		ttl time.Duration
		err error
	)
	if client.IsCluster {
		ttl, err = client.Cluster.TTL(key).Result()
	} else {
		ttl, err = client.Redis.TTL(key).Result()
	}

	if err != nil {
		return "", ehand.WithStackError(err)
	}
	secs := ttl.Seconds()
	if secs == -1 {
		return "-1", nil // expire not set
	} else if secs == -2 {
		return "-2", nil // already expired
	} else {
		return time.Now().Add(ttl).Format(timeFmt), nil
	}

}

func (client ClusterAndRedisClient) GetStringValue(key string, ifBytes bool) (string, int, error) {
	var (
		val string
		err error
		cnt int
	)
	if client.IsCluster {
		val, err = client.Cluster.Get(key).Result()
	} else {
		val, err = client.Redis.Get(key).Result()
	}
	if err != nil {
		return "", 0, ehand.WithStackError(err)
	}

	if ifBytes {
		cnt = len(key) + len(val)
	} else {
		cnt = 0
	}

	return val, cnt, nil
}

func (client ClusterAndRedisClient) DeleteKeyString(key string) error {
	var (
		err error
	)
	if client.IsCluster {
		_, err = client.Cluster.Del(key).Result()
	} else {
		_, err = client.Redis.Del(key).Result()
	}

	if err != nil {
		return ehand.WithStackError(err)
	} else {
		return nil
	}

}

func (client ClusterAndRedisClient) GetListLen(key string) (int64, error) {
	var (
		err    error
		length int64
	)

	if client.IsCluster {
		length, err = client.Cluster.LLen(key).Result()
	} else {
		length, err = client.Redis.LLen(key).Result()
	}

	if err != nil {
		return 0, ehand.WithStackError(err)
	}

	return length, nil

}

/*
param: key name, key length, count of elements to get each time, sleep after this process elementBatch element, esleep time for elementInterval in microsecond, if sleep, if calculate memory used in bytes
return: value, bytes of the key, biggest element, bytes of the biggest element, error

*/
func (client ClusterAndRedisClient) GetListValue(key string, length int64, eachCnt int64, elementBatch int,
	elementInterval time.Duration, ifSleep bool, ifBytes bool) ([]string, int, string, int, error) {
	var (
		err        error
		val        []string
		tmpArr     []string
		i          int64 = 0
		stop       int64
		bytesCnt   int = len(key)
		maxElement string
		maxBytes   int = 0
		oneBytes   int = 0
		actualCnt  int = 0
		alreadyCnt int = 0
	)

	for {
		if i >= length {
			break
		}
		if i+eachCnt >= length {
			stop = length
		} else {
			stop = i + eachCnt
		}
		if client.IsCluster {
			tmpArr, err = client.Cluster.LRange(key, i, stop).Result()
		} else {
			tmpArr, err = client.Redis.LRange(key, i, stop).Result()
		}

		if err != nil {
			return val, bytesCnt, maxElement, maxBytes, ehand.WithStackError(err)
		}
		if ifBytes {
			for _, v := range tmpArr {
				oneBytes = len(v)
				bytesCnt += oneBytes
				if oneBytes > maxBytes {
					maxElement = v
					maxBytes = oneBytes
				}
			}
		}
		val = append(val, tmpArr...)

		actualCnt = len(tmpArr)
		i += int64(actualCnt) + 1

		if ifSleep {
			alreadyCnt += actualCnt
			if alreadyCnt > elementBatch {
				time.Sleep(elementInterval)
				alreadyCnt = 0
			}

		}

	}

	return val, bytesCnt, maxElement, maxBytes, nil
}

/*
param: 	param: key name, key length, count of elements to delete each time, sleep after this process elementBatch element,
		sleep time for elementInterval in microsecond, if sleep
return: error
*/

func (client ClusterAndRedisClient) DeleteKeyList(key string, length int64, eachCnt int64, elementBatch int64,
	elementInterval time.Duration, ifSleep bool) error {

	var (
		err         error
		i           int64 = 0
		sidx        int64 = 0
		eidx        int64
		oneBatchCnt int64 = 0
	)

	for {
		eidx = 0 - eachCnt - 1
		if client.IsCluster {
			_, err = client.Cluster.LTrim(key, sidx, eidx).Result()
		} else {
			_, err = client.Redis.LTrim(key, sidx, eidx).Result()
		}
		if err != nil {
			return ehand.WithStackError(err)
		}
		i += eachCnt
		if i > length {
			break
		}
		if ifSleep {
			oneBatchCnt += eachCnt
			if oneBatchCnt >= elementBatch {
				time.Sleep(elementInterval)
				oneBatchCnt = 0
			}
		}

	}

	//the key should be not exists now, but Maybe other client push element at the same time. delete it whatever. delete unexists key not result in error
	if client.IsCluster {
		_, err = client.Cluster.Del(key).Result()
	} else {
		_, err = client.Redis.Del(key).Result()
	}

	if err != nil {
		return ehand.WithStackError(err)
	} else {
		return nil
	}
}

/*
param:  key name, count of elements to get each time, sleep after processing elementBatch elements,
		sleep time for elementInterval in microsecond, if sleep, if calculate memory used in bytes
return: value, bytes of the key, biggest element, bytes of the biggest element, error

*/

func (client ClusterAndRedisClient) GetHashValue(key string, eachCnt int64, elementBatch int,
	elementInterval time.Duration, ifSleep bool, ifBytes bool) (map[string]interface{},
	int, string, int, int, error) {
	var (
		cursor     uint64 = 0
		eKeys      []string
		values     map[string]interface{} = map[string]interface{}{}
		bytesCnt   int                    = len(key)
		maxElement string
		maxBytes   int = 0
		oneBytes   int = 0
		alreadyCnt int = 0
		elementCnt int = 0

		err error
	)

	for {

		if client.IsCluster {
			eKeys, cursor, err = client.Cluster.HScan(key, cursor, "", eachCnt).Result()

		} else {
			eKeys, cursor, err = client.Redis.HScan(key, cursor, "", eachCnt).Result()

		}
		if err != nil {
			return values, bytesCnt, maxElement, maxBytes, elementCnt, ehand.WithStackError(err)
		}

		for i := 0; i < len(eKeys); i += 2 {
			if ifSleep {
				alreadyCnt++
			}
			elementCnt++
			values[eKeys[i]] = eKeys[i+1]
			if ifBytes {

				jBytes, err := json.Marshal(eKeys[i+1])
				if err != nil {
					return values, bytesCnt, maxElement, maxBytes, elementCnt, ehand.WithStackError(err)
				}
				oneBytes = len(jBytes)
				bytesCnt += oneBytes + len(eKeys[i])
				if oneBytes > maxBytes {
					maxBytes = oneBytes
					maxElement = eKeys[i]
				}
			}

		}

		if cursor == 0 {
			break
		}

		if ifSleep {
			if alreadyCnt > elementBatch {
				time.Sleep(elementInterval)
				alreadyCnt = 0
			}

		}

	}

	return values, bytesCnt, maxElement, maxBytes, elementCnt, nil
}

/*
param:  key name, count of elements to get each time, sleep after processing elementBatch elements,
		sleep for elementInterval in microsecond, if calculate memory used in bytes
return: value, bytes of the key, biggest element, bytes of the biggest element, error

*/
func (client ClusterAndRedisClient) GetSetValue(key string, eachCnt int64, elementBatch int,
	elementInterval time.Duration, ifSleep bool, ifBytes bool) ([]string, int, string, int, int, error) {
	var (
		err        error
		cursor     uint64 = 0
		oneVals    []string
		values     []string = []string{}
		bytesCnt   int      = len(key)
		maxElement string
		maxBytes   int = 0
		oneByte    int = 0
		alreadyCnt int = 0

		elementCnt int = 0
	)

	for {
		if client.IsCluster {
			oneVals, cursor, err = client.Cluster.SScan(key, cursor, "", eachCnt).Result()
		} else {
			oneVals, cursor, err = client.Redis.SScan(key, cursor, "", eachCnt).Result()
		}
		if err != nil {
			return values, bytesCnt, maxElement, maxBytes, elementCnt, ehand.WithStackError(err)
		}
		elementCnt += len(oneVals)

		values = append(values, oneVals...)

		if ifBytes {
			for _, v := range oneVals {
				oneByte = len(v)
				bytesCnt += oneByte
				if oneByte > maxBytes {
					maxBytes = oneByte
					maxElement = v
				}

			}
		}
		if cursor == 0 {
			break
		}

		if ifSleep {
			alreadyCnt += len(oneVals)
			if alreadyCnt > elementBatch {
				time.Sleep(elementInterval)
			}

		}

	}

	return values, bytesCnt, maxElement, maxBytes, elementCnt, nil
}

/*
param: key name, count of elements to get each time, sleep time for eachCnt in microsecond, if calculate memory used in bytes
return: value, bytes of the key, biggest element, bytes of the biggest element, error

*/

func (client ClusterAndRedisClient) GetZsetValue(key string, eachCnt int64, elementBatch int,
	elementInterval time.Duration, ifSleep bool, ifBytes bool) (map[string]float64, int, string, int, int, error) {
	var (
		err        error
		cursor     uint64 = 0
		oneKeys    []string
		values     map[string]float64 = map[string]float64{}
		bytesCnt   int                = len(key)
		maxElement string
		maxBytes   int = 0
		oneByte    int
		score      float64
		alreadyCnt int = 0
		elementCnt int = 0
	)

	for {
		if client.IsCluster {
			oneKeys, cursor, err = client.Cluster.ZScan(key, cursor, "", eachCnt).Result()
		} else {
			oneKeys, cursor, err = client.Redis.ZScan(key, cursor, "", eachCnt).Result()
		}
		if err != nil {
			return values, bytesCnt, maxElement, maxBytes, elementCnt, ehand.WithStackError(err)
		}

		for i := 0; i < len(oneKeys); i += 2 {
			if ifSleep {
				alreadyCnt++
			}
			elementCnt++
			score, err = strconv.ParseFloat(oneKeys[i+1], 64)
			if err != nil {
				return values, bytesCnt, maxElement, maxBytes, elementCnt, ehand.WithStackError(err)
			}
			values[oneKeys[i]] = score
			if ifBytes {
				oneByte = len(oneKeys[i])
				bytesCnt += oneByte + 8 // each float64 number takes 8 bytes
				if oneByte > maxBytes {
					maxBytes = oneByte
					maxElement = oneKeys[i]
				}
			}

		}

		if cursor == 0 {
			break
		}

		if ifSleep {
			if alreadyCnt > elementBatch {
				time.Sleep(elementInterval)
			}
		}

	}
	maxBytes += 8
	return values, bytesCnt, maxElement, maxBytes, elementCnt, nil
}

/*
 get one key info and value
*/
func (client ClusterAndRedisClient) GetKeyValue(key string, eachCnt int64, elementBatch int, elementInterval time.Duration, ifSleep bool,
	ifCountMem bool, ifNeedValue bool) (RedisKey, error) {
	var (
		kv         RedisKey = RedisKey{Key: key}
		err        error
		tp         string
		exp        string
		values     interface{}
		bytesCnt   int
		maxElement string = ""
		maxBytes   int    = 0
		elementCnt int    = 0
	)

	tp, err = client.GetKeyType(key)
	//fmt.Printf("type of %s: %s\n", key, tp)
	if err != nil {
		return kv, err
	}
	kv.Type = tp

	exp, err = client.GetKeyTTL(key, constvar.DATETIME_FORMAT_NOSPACE)
	if err != nil {
		return kv, err
	}
	kv.Expire = exp
	// already expire
	if exp == "-2" {
		return kv, nil
	}

	switch tp {
	case "string":
		values, bytesCnt, err = client.GetStringValue(key, ifCountMem)
		if err != nil {
			return kv, err
		}
		elementCnt = 1
		//kv.Bytes = bytesCnt

		//kv.Value = values

	case "list":
		length, err := client.GetListLen(key)
		if err != nil {
			return kv, err
		}
		values, bytesCnt, maxElement, maxBytes, err = client.GetListValue(key, length, eachCnt, elementBatch, elementInterval, ifSleep, ifCountMem)
		if err != nil {
			return kv, err
		}
		elementCnt = int(length)

	case "hash":
		values, bytesCnt, maxElement, maxBytes, elementCnt, err = client.GetHashValue(key, eachCnt, elementBatch, elementInterval, ifSleep, ifCountMem)
		if err != nil {
			return kv, err
		}

	case "set":
		values, bytesCnt, maxElement, maxBytes, elementCnt, err = client.GetSetValue(key, eachCnt, elementBatch, elementInterval, ifSleep, ifCountMem)
		if err != nil {
			return kv, err
		}
	case "zset":
		values, bytesCnt, maxElement, maxBytes, elementCnt, err = client.GetZsetValue(key, eachCnt, elementBatch, elementInterval, ifSleep, ifCountMem)
		if err != nil {
			return kv, err
		}

	}
	kv.Bytes = bytesCnt
	kv.MaxElement = maxElement
	kv.MaxElementBytes = maxBytes
	kv.ElementCnt = elementCnt

	if ifNeedValue {
		kv.Value = values
	} else {
		values = nil
	}
	return kv, nil

}

func (client ClusterAndRedisClient) GetRedisInfo(section string) (map[string]string, error) {
	var (
		str    string
		err    error
		result map[string]string = map[string]string{}
	)

	str, err = client.Redis.Info(section).Result()
	if err != nil {
		return result, ehand.WithStackError(err)
	}
	str = strings.TrimSpace(str)
	arr := strings.Split(str, "\n")
	for _, line := range arr {
		if strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimSpace(line)
		tarr := strings.Split(line, ":")
		if len(tarr) != 2 {
			continue
		}
		result[tarr[0]] = tarr[1]
	}

	return result, nil

}

//slave0:ip=10.199.203.210,port=6683,state=online,offset=2047641,lag=1
func (client ClusterAndRedisClient) GetSlaveReplInfo(infoStr string) (SlaveInfo, error) {
	var (
		info SlaveInfo = SlaveInfo{}
		err  error
	)

	infoStr = strings.TrimSpace(infoStr)
	arr := strings.Split(infoStr, ",")

	for _, str := range arr {
		tarr := strings.Split(str, "=")
		switch tarr[0] {
		case "ip":
			info.Addr.Host = tarr[1]
		case "port":
			info.Addr.Port, err = strconv.Atoi(tarr[1])
			if err != nil {
				return info, ehand.WithStackError(err)
			}
		case "state":
			if tarr[1] == "online" {
				info.Replicating = true
			} else {
				info.Replicating = false
			}
		case "offset":
			info.Offset, err = strconv.Atoi(tarr[1])
			if err != nil {
				return info, ehand.WithStackError(err)
			}
		case "lag":
			info.Lag, err = strconv.Atoi(tarr[1])
			if err != nil {
				return info, ehand.WithStackError(err)
			}
		}
	}
	return info, nil
}

/*
# Replication
role:master
connected_slaves:1
slave0:ip=10.199.203.210,port=6683,state=online,offset=2047641,lag=1
master_repl_offset:2047641

role:slave
master_host:10.199.203.208
master_port:6683
master_link_status:up
master_last_io_seconds_ago:1
master_sync_in_progress:0
slave_repl_offset:2047571
slave_priority:100

*/

func (client ClusterAndRedisClient) GetRedisInfoRole() (RedisRoleInfo, error) {
	var (
		err   error
		info  RedisRoleInfo = RedisRoleInfo{}
		reMap map[string]string
		sInfo SlaveInfo
		//mtch  *regexp.Regexp = regexp.MustCompile(`slave\d+`)
	)

	reMap, err = client.GetRedisInfo("replication")
	if err != nil {
		return info, err
	}

	if reMap["role"] == "master" {
		info.Role = "master"
		info.Slaves = []SlaveInfo{}
		for k, v := range reMap {
			if G_regexp_info_replication_slave.MatchString(k) {
				//slave0:ip=10.199.203.210,port=6683,state=online,offset=2047641,lag=1
				sInfo, err = client.GetSlaveReplInfo(v)
				if err != nil {
					return info, err
				}
				info.Slaves = append(info.Slaves, sInfo)
			}
		}

	} else {
		info.Role = "slave"
		info.MasterAddr.Host = reMap["master_host"]
		info.MasterAddr.Port, err = strconv.Atoi(reMap["master_port"])
		if err != nil {
			return info, ehand.WithStackError(err)
		}
		if reMap["master_link_status"] == "up" {
			info.Replicating = true
		} else {
			info.Replicating = false
		}
	}

	return info, nil
}

/*

# Server
redis_version:3.0.3
redis_git_sha1:00000000
redis_git_dirty:0
redis_build_id:eff44f0690b88743
redis_mode:cluster

redis_version:3.0.3
redis_git_sha1:00000000
redis_git_dirty:0
redis_build_id:6d98d05e9f9e8598
redis_mode:standalone
*/

func (client ClusterAndRedisClient) CheckIfCluster() (bool, error) {
	var (
		err   error
		reMap map[string]string
	)

	reMap, err = client.GetRedisInfo("server")
	if err != nil {
		return false, err
	}

	if reMap["redis_mode"] == "cluster" {
		return true, nil
	} else {
		return false, nil
	}

}

func (client ClusterAndRedisClient) GetLatestSlave(slaves []SlaveInfo) (SlaveInfo, error) {
	var (
		lestLag int = -1
		sl      SlaveInfo
		//ok      bool = false
	)
	for _, s := range slaves {
		if !s.Replicating {
			continue
		}

		if lestLag == -1 || s.Lag <= lestLag {
			sl = s
			//ok = tru
			lestLag = s.Lag
		}
	}
	if lestLag == -1 {
		return sl, ehand.WithStackError(fmt.Errorf("no online slave found"))
	}

	return sl, nil

}

func GetRedisAddr(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}

/*
param: addrs of masters
return: latest slave addrs of masters(map)， latest slave addrs of masters(list), masters without replicating slave, error
*/

func GetLatestSlaveOfMasters(masters []string) (map[string]string, []string, []string, error) {
	var (
		redisConf      ConfRedis
		redisClient    *redis.Client
		client         ClusterAndRedisClient
		err            error
		slavesMap      map[string]string = map[string]string{}
		slaveList      []string          = []string{}
		mastersNoSlave []string          = []string{}
		roleInfo       RedisRoleInfo
		slaveInfo      SlaveInfo
	)

	for _, m := range masters {
		redisConf = ConfRedis{Addr: m, Database: 0}
		redisClient, err = redisConf.CreateNewClientRedis()
		if err != nil {
			return slavesMap, slaveList, mastersNoSlave, err
		}
		client = ClusterAndRedisClient{Redis: redisClient, IsCluster: false}
		roleInfo, err = client.GetRedisInfoRole()
		if err != nil {
			return slavesMap, slaveList, mastersNoSlave, err
		}
		if len(roleInfo.Slaves) > 0 {
			slaveInfo, err = client.GetLatestSlave(roleInfo.Slaves)
			if err != nil {
				// no online slaves
				mastersNoSlave = append(mastersNoSlave, m)
				continue
			}
			oneAddr := fmt.Sprintf("%s:%d", slaveInfo.Addr.Host, slaveInfo.Addr.Port)
			slavesMap[m] = oneAddr
			slaveList = append(slaveList, oneAddr)

		} else {
			mastersNoSlave = append(mastersNoSlave, m)
		}
		redisClient.Close()

	}
	return slavesMap, slaveList, mastersNoSlave, nil

}

type RedisInfoAll struct {
	Redis_version              string
	IsCluster                  int64 // cluster:1, redis:0
	Process_id                 uint64
	Total_connections_received uint64
	Connected_clients          uint64
	Rejected_connections       uint64
	Blocked_clients            uint64
	Client_longest_output_list uint64
	Client_biggest_input_buf   uint64
	Total_commands_processed   uint64
	Instantaneous_ops_per_sec  float64
	Keyspace_hits              uint64
	Keyspace_misses            uint64
	Expired_keys               uint64
	Evicted_keys               uint64
	Keys_alldb                 uint64 // number of keys of all db
	Keys_expires               uint64 // number of keys with expiration of all db
	Total_net_input_bytes      uint64
	Total_net_output_bytes     uint64
	Instantaneous_input_kbps   float64
	Instantaneous_output_kbps  float64
	Used_memory                uint64
	Used_memory_rss            uint64
	Used_memory_peak           uint64
	Used_memory_lua            uint64
	Mem_fragmentation_ratio    float64
	LoadingRdb                 int64
	Rdb_bgsave_in_progress     int64
	Rdb_last_bgsave_status     int64 // ok: 0, error: 1
	Aof_rewrite_in_progress    int64
	Aof_last_bgrewrite_status  int64 // ok: 0, error: 1
	Aof_last_write_status      int64 // ok: 0, error: 1
	Pubsub_channels            uint64
	Pubsub_patterns            uint64
	Role                       string
	Connected_slaves           uint64
	Master_host                string
	Master_port                uint64
	Master_link_status         int64 // up: 1, else: 0
	SlaveLag                   int64
	Master_sync_in_progress    int64
}

// selfIps: the ips of the slave, used to get slave lag from master

func (client ClusterAndRedisClient) GetRedisInfoAll(selfIps []string, port int, ifGetSlaveLag bool) (RedisInfoAll, error) {
	var (
		err    error
		result map[string]string
		info   RedisInfoAll = RedisInfoAll{}
	)
	// server
	result, err = client.GetRedisInfo("server")
	if err != nil {
		return info, err
	}
	ParseInfoSever(result, &info)

	//clients
	result, err = client.GetRedisInfo("clients")
	if err != nil {
		return info, err
	}
	ParseInfoClient(result, &info)

	// memory
	result, err = client.GetRedisInfo("memory")
	if err != nil {
		return info, err
	}
	ParseInfoMemory(result, &info)

	// persistence
	result, err = client.GetRedisInfo("persistence")
	if err != nil {
		return info, err
	}
	ParseInfoPersistent(result, &info)

	//stats
	result, err = client.GetRedisInfo("stats")
	if err != nil {
		return info, err
	}
	ParseInfoStats(result, &info)

	//replication
	result, err = client.GetRedisInfo("replication")
	if err != nil {
		return info, err
	}
	ParseInfoReplication(result, &info)

	if ifGetSlaveLag && info.Role == "slave" {
		if port != 0 {
			// skip get repilication lag
			redisCnf := ConfRedis{Addr: GetRedisAddr(info.Master_host, int(info.Master_port)), Database: 0}
			redisClient, err := redisCnf.CreateNewClientRedis()
			if err == nil {

				genClient := ClusterAndRedisClient{Redis: redisClient, IsCluster: false}
				sIps := map[string]int{}
				for _, p := range selfIps {
					sIps[p] = port
				}
				oneline, lag, err := genClient.GetSlavesReplicationStatus(sIps)
				if err == nil {
					info.Master_link_status = oneline
					info.SlaveLag = lag
				}
				redisClient.Close()

			}
			redisClient = nil
		}

	}

	//keyspace
	result, err = client.GetRedisInfo("keyspace")
	if err != nil {
		return info, err
	}
	//mStr := regexp.MustCompile(`db\d+`)
	ParseInfoKeyspace(result, &info)

	return info, nil
}

func ParseInfoKeyspace(result map[string]string, info *RedisInfoAll) {

	var (
		k      string
		v      string
		keyCnt uint64
		expCnt uint64

		err error
	)
	for k, v = range result {
		if G_regexp_info_keyspace_db.MatchString(k) {
			keyCnt, expCnt, _, err = ParseDBkeyspce(v)
			if err != nil {
				continue
			}
			info.Keys_alldb += keyCnt
			info.Keys_expires += expCnt
		}
	}

}

func ParseInfoSever(result map[string]string, info *RedisInfoAll) {

	var (
		k       string
		v       string
		tmpUint uint64

		err error
	)

	for k, v = range result {
		switch k {
		case "redis_mode":
			if v == "cluster" {
				info.IsCluster = 1
			} else {
				info.IsCluster = 0
			}
		case "process_id":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Process_id = tmpUint
			}
		}
	}
}

func ParseInfoClient(result map[string]string, info *RedisInfoAll) {

	var (
		k       string
		v       string
		tmpUint uint64

		err error
	)

	for k, v = range result {
		switch k {
		case "connected_clients":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Connected_clients = tmpUint
			}
		case "client_longest_output_list":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Client_longest_output_list = tmpUint
			}
		case "client_biggest_input_buf":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Client_biggest_input_buf = tmpUint
			}
		case "blocked_clients":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Blocked_clients = tmpUint
			}
		}
	}
}

func ParseInfoMemory(result map[string]string, info *RedisInfoAll) {

	var (
		k        string
		v        string
		tmpUint  uint64
		tmpFloat float64
		err      error
	)

	for k, v = range result {
		switch k {
		case "used_memory":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Used_memory = tmpUint
			}
		case "used_memory_rss":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Used_memory_rss = tmpUint
			}
		case "used_memory_peak":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Used_memory_peak = tmpUint
			}
		case "used_memory_lua":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Used_memory_lua = tmpUint
			}
		case "mem_fragmentation_ratio":
			tmpFloat, err = strconv.ParseFloat(v, 64)
			if err == nil {
				info.Mem_fragmentation_ratio = tmpFloat
			}
		}
	}
}

func ParseInfoPersistent(result map[string]string, info *RedisInfoAll) {

	var (
		k      string
		v      string
		tmpInt int64
		err    error
	)
	for k, v = range result {
		switch k {
		case "loading":
			tmpInt, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				info.LoadingRdb = tmpInt
			}
		case "rdb_bgsave_in_progress":
			tmpInt, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				info.Rdb_bgsave_in_progress = tmpInt
			}
		case "rdb_last_bgsave_status":
			if v == "ok" {
				info.Rdb_last_bgsave_status = 0
			} else {
				info.Rdb_last_bgsave_status = 1
			}
		case "aof_rewrite_in_progress":
			tmpInt, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				info.Aof_rewrite_in_progress = tmpInt
			}
		case "aof_last_bgrewrite_status":
			if v == "ok" {
				info.Aof_last_bgrewrite_status = 0
			} else {
				info.Aof_last_bgrewrite_status = 1
			}
		case "aof_last_write_status":
			if v == "ok" {
				info.Aof_last_write_status = 0
			} else {
				info.Aof_last_write_status = 1
			}
		}
	}

}

func ParseInfoStats(result map[string]string, info *RedisInfoAll) {

	var (
		k        string
		v        string
		tmpUint  uint64
		tmpFloat float64
		err      error
	)
	for k, v = range result {
		switch k {
		case "total_connections_received":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Total_connections_received = tmpUint
			}
		case "total_commands_processed":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Total_commands_processed = tmpUint
			}
		case "instantaneous_ops_per_sec":
			tmpFloat, err = strconv.ParseFloat(v, 64)
			if err == nil {
				info.Instantaneous_ops_per_sec = tmpFloat
			}
		case "total_net_input_bytes":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Total_net_input_bytes = tmpUint
			}
		case "total_net_output_bytes":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Total_net_output_bytes = tmpUint
			}
		case "instantaneous_input_kbps":
			tmpFloat, err = strconv.ParseFloat(v, 64)
			if err == nil {
				info.Instantaneous_input_kbps = tmpFloat
			}
		case "instantaneous_output_kbps":
			tmpFloat, err = strconv.ParseFloat(v, 64)
			if err == nil {
				info.Instantaneous_output_kbps = tmpFloat
			}
		case "rejected_connections":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Rejected_connections = tmpUint
			}
		case "expired_keys":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Expired_keys = tmpUint
			}
		case "evicted_keys":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Evicted_keys = tmpUint
			}
		case "keyspace_hits":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Keyspace_hits = tmpUint
			}
		case "keyspace_misses":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Keyspace_misses = tmpUint
			}
		case "pubsub_channels":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Pubsub_channels = tmpUint
			}
		case "pubsub_patterns":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Pubsub_patterns = tmpUint
			}
		}
	}

}

func ParseInfoReplication(result map[string]string, info *RedisInfoAll) {

	var (
		k       string
		v       string
		tmpUint uint64
		tmpInt  int64
		err     error
	)
	for k, v = range result {
		switch k {
		case "role":
			info.Role = v
		case "connected_slaves":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Connected_slaves = tmpUint
			}
		case "master_host":
			info.Master_host = v
		case "master_port":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Master_port = tmpUint
			}
		case "master_link_status":
			if v == "up" {
				info.Master_link_status = 1
			} else {
				info.Master_link_status = 0
			}
		case "master_sync_in_progress":
			tmpInt, err = strconv.ParseInt(v, 10, 64)
			if err == nil {
				info.Master_sync_in_progress = tmpInt
			}
		}
	}

}

//db0:keys=4,expires=1,avg_ttl=137614
// return: keys, expires, avg_ttl

func ParseDBkeyspce(line string) (uint64, uint64, float64, error) {

	var (
		keyCnt uint64
		expCnt uint64
		avgTTL float64
		err    error
	)
	line = strings.TrimSpace(line)
	if strings.Contains(line, ":") {
		arr := strings.Split(line, ":")
		line = arr[1]
	}
	tmpArr := strings.Split(line, ",")
	for _, v := range tmpArr {
		tArr := strings.Split(v, "=")
		if tArr[0] == "keys" {
			keyCnt, err = strconv.ParseUint(tArr[1], 10, 64)
			if err != nil {
				return keyCnt, expCnt, avgTTL, err
			}
		} else if tArr[0] == "expires" {
			expCnt, err = strconv.ParseUint(tArr[1], 10, 64)
			if err != nil {
				return keyCnt, expCnt, avgTTL, err
			}
		} else if tArr[0] == "avg_ttl" {
			avgTTL, err = strconv.ParseFloat(tArr[1], 64)
			if err != nil {
				return keyCnt, expCnt, avgTTL, err
			}
		}
	}
	return keyCnt, expCnt, avgTTL, nil
}

//slave0:ip=10.199.203.210,port=6683,state=online,offset=2047641,lag=1
// return : online: 1, offline: 0,
//			lag

func (client ClusterAndRedisClient) GetSlavesReplicationStatus(slaveIps map[string]int) (int64, int64, error) {
	var (
		result map[string]string
		err    error
		online int64 = 1
		lag    int64 = 0
		sInfo  SlaveInfo
		//mtch   *regexp.Regexp = regexp.MustCompile(`slave\d+`)
		//found  bool           = false
	)
	result, err = client.GetRedisInfo("replication")
	if err != nil {
		return online, lag, err
	}
	for k, v := range result {
		if G_regexp_info_replication_slave.MatchString(k) {
			sInfo, err = client.GetSlaveReplInfo(v)
			if err != nil {
				continue
			}
			for ip, pt := range slaveIps {
				if sInfo.Addr.Host == ip && sInfo.Addr.Port == pt {
					if sInfo.Replicating {
						online = 1
					} else {
						online = 0
					}
					lag = int64(sInfo.Lag)
					return online, lag, nil
				}
			}
		}
	}
	return online, lag, nil
}

type RedisConfVar struct {
	Maxmemory                     uint64
	Cluster_node_timeout          uint64
	Cluster_require_full_coverage int64 // yes: 1, no: 0
	Slave_serve_stale_data        int64 // yes:1, no:0
	Slave_read_only               int64 // yes:1, no:0
	Stop_writes_on_bgsave_error   int64 // yes:1, no:0
}

func (client ClusterAndRedisClient) GetRedisConfVars(ifErrBreak bool) (RedisConfVar, error) {
	var (
		err     error
		confVar RedisConfVar = RedisConfVar{}

		tmpUint uint64
		tmpStr  string
		//ok      bool
	)

	tmpUint, err = client.GetRedisOneVarUint64("maxmemory")
	if err != nil {
		if ifErrBreak {
			return confVar, err
		}
	} else {
		confVar.Maxmemory = tmpUint
	}

	tmpUint, err = client.GetRedisOneVarUint64("cluster-node-timeout")
	if err != nil {
		if ifErrBreak {
			return confVar, err
		}
	} else {
		confVar.Cluster_node_timeout = tmpUint
	}

	tmpStr, err = client.GetRedisOneVarString("cluster-require-full-coverage")
	if err != nil {
		if ifErrBreak {
			return confVar, err
		}
	} else {
		//tmpStr = strings.TrimSpace(tmpStr)
		tmpStr = strings.ToLower(tmpStr)
		if tmpStr == "yes" {
			confVar.Cluster_require_full_coverage = 1
		} else {
			confVar.Cluster_require_full_coverage = 0
		}
	}

	tmpStr, err = client.GetRedisOneVarString("slave-serve-stale-data")
	if err != nil {
		if ifErrBreak {
			return confVar, err
		}
	} else {
		tmpStr = strings.ToLower(tmpStr)
		if tmpStr == "yes" {
			confVar.Slave_serve_stale_data = 1
		} else {
			confVar.Slave_serve_stale_data = 0
		}
	}

	tmpStr, err = client.GetRedisOneVarString("slave-read-only")
	if err != nil {
		if ifErrBreak {
			return confVar, err
		}
	} else {
		tmpStr = strings.ToLower(tmpStr)
		if tmpStr == "yes" {
			confVar.Slave_read_only = 1
		} else {
			confVar.Slave_read_only = 0
		}
	}

	tmpStr, err = client.GetRedisOneVarString("stop-writes-on-bgsave-error")
	if err != nil {
		if ifErrBreak {
			return confVar, err
		}
	} else {
		tmpStr = strings.ToLower(tmpStr)
		if tmpStr == "yes" {
			confVar.Stop_writes_on_bgsave_error = 1
		} else {
			confVar.Stop_writes_on_bgsave_error = 0
		}
	}

	return confVar, nil

}

func (client ClusterAndRedisClient) GetRedisOneVarString(name string) (string, error) {
	result, err := client.Redis.ConfigGet(name).Result()
	if err != nil {
		return "", ehand.WithStackError(err)
	}
	if len(result) < 2 {
		return "", ehand.WithStackError(fmt.Errorf("unsupported var %s: %v", name, result))
	}
	tmpStr, ok := result[1].(string)
	if ok {
		return strings.TrimSpace(tmpStr), nil
	} else {
		return "", ehand.WithStackError(fmt.Errorf("fail to convert %v to string", result[1]))
	}

}

func (client ClusterAndRedisClient) GetRedisOneVarUint64(name string) (uint64, error) {
	tmpStr, err := client.GetRedisOneVarString(name)
	if err != nil {
		return 0, err
	}
	tmpUint, err := strconv.ParseUint(tmpStr, 10, 64)
	if err != nil {
		return 0, ehand.WithStackError(fmt.Errorf("error to convert %s to uint64: %s", tmpStr, err))
	}
	return tmpUint, nil

}

func (client ClusterAndRedisClient) GetRedisOneVarInt64(name string) (int64, error) {
	tmpStr, err := client.GetRedisOneVarString(name)
	if err != nil {
		return 0, err
	}
	tmpInt, err := strconv.ParseInt(tmpStr, 10, 64)
	if err != nil {
		return 0, ehand.WithStackError(fmt.Errorf("error to convert %s to int64: %s", tmpStr, err))
	}
	return tmpInt, nil

}

type ClusterInfoAll struct {
	Cluster_state          int64 // OK: 1, 0: not ok
	Cluster_slots_assigned uint64
	Cluster_slots_ok       uint64
	Cluster_slots_pfail    uint64
	Cluster_slots_fail     uint64
	/*
		Cluster_known_nodes             uint64
		Cluster_size                    uint64
		Cluster_current_epoch           uint64
		Cluster_my_epoch                uint64
		Cluster_stats_messages_sent     uint64
		Cluster_stats_messages_received uint64
	*/
}

func (client ClusterAndRedisClient) GetClusterInfoAll() (ClusterInfoAll, error) {
	var (
		err  error
		info ClusterInfoAll = ClusterInfoAll{}

		tmpUint uint64
		result  map[string]string
	)

	result, err = client.GetClusterInfoString()
	if err != nil {
		return info, err
	}
	for k, v := range result {
		switch k {
		case "cluster_state":
			if v == "ok" {
				info.Cluster_state = 1
			} else {
				info.Cluster_state = 0
			}
		case "cluster_slots_assigned":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Cluster_slots_assigned = tmpUint
			}
		case "cluster_slots_ok":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Cluster_slots_ok = tmpUint
			}
		case "cluster_slots_pfail":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Cluster_slots_pfail = tmpUint
			}
		case "cluster_slots_fail":
			tmpUint, err = strconv.ParseUint(v, 10, 64)
			if err == nil {
				info.Cluster_slots_fail = tmpUint
			}
		}
	}

	return info, nil

}

func (client ClusterAndRedisClient) GetClusterInfoString() (map[string]string, error) {
	var (
		err    error
		tmpStr string
		result map[string]string = map[string]string{}
	)

	tmpStr, err = client.Redis.ClusterInfo().Result()
	if err != nil {
		return result, ehand.WithStackError(err)
	}
	tmpStr = strings.TrimSpace(tmpStr)
	if tmpStr == "" {
		return result, ehand.WithStackError(fmt.Errorf("cluster info return empty"))
	}
	arr := strings.Split(tmpStr, "\n")
	for _, line := range arr {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		tmpArr := strings.Split(line, ":")
		if len(tmpArr) != 2 {
			continue
		}
		result[tmpArr[0]] = result[tmpArr[1]]
	}

	if len(result) == 0 {
		return result, ehand.WithStackError(fmt.Errorf("cluster info return none of valid result"))
	}
	return result, nil
}

func (client ClusterAndRedisClient) RedisPing() bool {
	var (
		err    error
		result string
	)
	if client.IsCluster {
		result, err = client.Cluster.Ping().Result()
	} else {
		result, err = client.Redis.Ping().Result()
	}
	if err != nil {
		return false
	}
	result = strings.ToUpper(strings.TrimSpace(result))
	if result == "PONG" {
		return true
	} else {
		return false
	}
}
