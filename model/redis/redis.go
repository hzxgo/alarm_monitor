package redis

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	"alarm_monitor/config"

	"github.com/cihub/seelog"
	"github.com/garyburd/redigo/redis"
)

var (
	redisMap   map[string]*redis.Pool
	redisMutex sync.Mutex
	pool       *redis.Pool
)

const (
	lock_key_suffix = "-redis-lock"
	lock_key_value  = "locked"
)

func init() {
	var err error
	cfg := config.GetConfig()
	redisMap = make(map[string]*redis.Pool)
	addr := fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port)
	pool, err = newRedisPool(addr, cfg.Redis.Db, cfg.Redis.Auth)
	if err != nil {
		seelog.Criticalf("redis init err: %v", err)
		return
	}
}

func GetPool() redis.Conn {
	return pool.Get()
}

func GetActiveCount() int {
	return pool.ActiveCount()
}

func Get(key string) (string, error) {
	c := pool.Get()
	defer c.Close()

	return redis.String(c.Do("GET", key))
}

func Set(key string, val interface{}) error {
	c := pool.Get()
	defer c.Close()

	_, err := c.Do("SET", key, val)
	return err
}

func Del(key string) error {
	c := pool.Get()
	defer c.Close()

	_, err := c.Do("DEL", key)
	return err
}

func Hmset(key string, mapping interface{}) error {
	c := pool.Get()
	defer c.Close()

	_, err := c.Do("HMSET", redis.Args{}.Add(key).AddFlat(mapping)...)
	return err
}

func Keys(pattern string) ([]string, error) {
	c := pool.Get()
	defer c.Close()

	return redis.Strings(c.Do("KEYS", pattern))
}

func Smembers(key string) ([]string, error) {
	c := pool.Get()
	defer c.Close()

	return redis.Strings(c.Do("SMEMBERS", key))
}

func Hget(key, field string) ([]byte, error) {
	c := pool.Get()
	defer c.Close()

	res, err := c.Do("HGET", key, field)
	if err != nil || res == nil {
		return nil, err
	}
	return res.([]byte), nil
}

func Hset(key, field string, val []byte) (bool, error) {
	c := pool.Get()
	defer c.Close()

	ok, err := redis.Bool(c.Do("HSET", key, field, string(val)))
	return ok, err
}

func Hdel(key, field string) error {
	c := pool.Get()
	defer c.Close()

	_, err := c.Do("HDEL", key, field)
	return err
}

func Hgetall(key string) (map[string]string, error) {
	c := pool.Get()
	defer c.Close()

	res, err := bytesSlice(c.Do("HGETALL", key))
	if err != nil {
		return nil, err
	}

	result := make(map[string]string)
	writeToContainer(res, reflect.ValueOf(result))

	return result, err
}

func LRange(key string, start int, end int) ([]string, error) {
	c := pool.Get()
	defer c.Close()

	return redis.Strings(c.Do("LRANGE", key, start, end))
}

func Lrem(key string, count int, val []byte) error {
	c := pool.Get()
	defer c.Close()

	_, err := c.Do("LREM", key, count, string(val))
	return err
}

func Lpush(key string, val []byte) error {
	c := pool.Get()
	defer c.Close()

	_, err := c.Do("LPUSH", key, string(val))
	return err
}

func Rpush(key string, val []byte) error {
	c := pool.Get()
	defer c.Close()

	_, err := c.Do("RPUSH", key, string(val))
	return err
}

func Rpop(key string) ([]byte, error) {
	c := pool.Get()
	defer c.Close()

	res, err := c.Do("RPOP", key)
	if err != nil || res == nil {
		return nil, err
	}
	return res.([]byte), nil
}

func Brpoplpush(src, dest string, timeout int) ([]byte, error) {
	c := pool.Get()
	defer c.Close()

	res, err := c.Do("BRPOPLPUSH", src, dest, timeout)
	if err != nil || res == nil {
		return nil, err
	}

	return res.([]byte), nil
}

func Do(cmd string, args ...interface{}) (reply interface{}, err error) {
	c := pool.Get()
	defer c.Close()

	return c.Do(cmd, args...)
}

func TryLock(key string, milliseconds int) (bool, error) {
	c := pool.Get()
	defer c.Close()

	_, err := redis.String(c.Do("SET", key+lock_key_suffix, lock_key_value, "PX", milliseconds, "NX"))
	if err == redis.ErrNil {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return true, nil
}

func UnLock(key string) error {
	c := pool.Get()
	defer c.Close()

	_, err := c.Do("DEL", key+lock_key_suffix)
	return err
}

// ---------------------------------------------------------------------------------------------------------------------

func newRedisPool(addr string, db string, password string) (*redis.Pool, error) {
	mapKey := fmt.Sprintf("%s:%s:%s", addr, db, password)
	if redisPool, ok := redisMap[mapKey]; ok {
		return redisPool, nil
	}

	redisPool := &redis.Pool{
		MaxIdle:     80,
		MaxActive:   10000,
		IdleTimeout: 60 * time.Second,
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", addr)
			if err != nil {
				return nil, err
			}
			if password != "" {
				_, err = conn.Do("AUTH", password)
				if err != nil {
					return nil, err
				}
			}
			if db != "" {
				conn.Do("SELECT", db)
			}
			return conn, err
		},
	}

	conn := redisPool.Get()
	if conn == nil {
		return nil, errors.New("can't get new redis conn")
	}

	_, err := redis.String(conn.Do("PING"))
	if err != nil {
		conn.Close()
		return nil, err
	}

	return redisPool, nil
}

func bytesSlice(reply interface{}, err error) ([][]byte, error) {
	if err != nil {
		return nil, err
	}
	switch reply := reply.(type) {
	case []interface{}:
		result := make([][]byte, len(reply))
		for i := range reply {
			if reply[i] == nil {
				continue
			}
			p, ok := reply[i].([]byte)
			if !ok {
				return nil, fmt.Errorf("redigo: Unexpected element type for []byte, got type %T", reply[i])
			}
			result[i] = p
		}
		return result, nil
	case nil:
		return nil, redis.ErrNil
	case redis.Error:
		return nil, reply
	}
	return nil, fmt.Errorf("redigo: Unexpected type for []byte, got type %T", reply)
}

func writeToContainer(data [][]byte, val reflect.Value) error {
	switch v := val; v.Kind() {
	case reflect.Ptr:
		return writeToContainer(data, reflect.Indirect(v))
	case reflect.Interface:
		return writeToContainer(data, v.Elem())
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return errors.New("redigo: Invalid map type")
		}
		elemtype := v.Type().Elem()
		for i := 0; i < len(data)/2; i++ {
			mk := reflect.ValueOf(string(data[i*2]))
			mv := reflect.New(elemtype).Elem()
			writeTo(data[i*2+1], mv)
			v.SetMapIndex(mk, mv)
		}
	case reflect.Struct:
		for i := 0; i < len(data)/2; i++ {
			name := string(data[i*2])
			field := v.FieldByName(name)
			if !field.IsValid() {
				continue
			}
			writeTo(data[i*2+1], field)
		}
	default:
		return errors.New("redigo: Invalid container type")
	}
	return nil
}

func writeTo(data []byte, val reflect.Value) error {
	s := string(data)
	switch v := val; v.Kind() {

	// if we're writing to an interace value, just set the byte data
	case reflect.Interface:
		v.Set(reflect.ValueOf(data))

	case reflect.Bool:
		b, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		v.SetBool(b)

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		v.SetInt(i)

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		ui, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return err
		}
		v.SetUint(ui)

	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}
		v.SetFloat(f)

	case reflect.String:
		v.SetString(s)

	case reflect.Slice:
		typ := v.Type()
		if typ.Elem().Kind() == reflect.Uint || typ.Elem().Kind() == reflect.Uint8 || typ.Elem().Kind() == reflect.Uint16 || typ.Elem().Kind() == reflect.Uint32 || typ.Elem().Kind() == reflect.Uint64 || typ.Elem().Kind() == reflect.Uintptr {
			v.Set(reflect.ValueOf(data))
		}
	}
	return nil
}
