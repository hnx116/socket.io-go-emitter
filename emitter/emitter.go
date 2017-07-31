package emitter

import (
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
	"gopkg.in/vmihailenco/msgpack.v1"
)

const (
	event              = 2
	binaryEvent        = 5
	redisPoolMaxIdle   = 80
	redisPoolMaxActive = 12000 // max number of connections
	uid                = "emitter"
)

// Options ...
type Options struct {
	Host      string
	Password  string
	Key       string
	RedisPool *redis.Pool
}

//Emitter ... Socket.io Emitter
type Emitter struct {
	_opts  Options
	_key   string
	_flags map[string]string
	_rooms map[string]bool
	_pool  *redis.Pool
}

// New ... Creates new Emitter using options
func New(opts Options) *Emitter {
	e := Emitter{_opts: opts}

	if opts.RedisPool != nil {
		e._pool = opts.RedisPool
	} else {
		if opts.Host == "" {
			panic("Missing redis `host`")
		}

		e._pool = newPool(opts)
	}

	if opts.Key != "" {
		e._key = fmt.Sprintf("%s", opts.Key)
	} else {
		e._key = "socket.io"
	}

	e._rooms = make(map[string]bool)
	e._flags = make(map[string]string)

	return &e
}

// In ... Limit emission to a certain `room`.`
func (e *Emitter) In(room string) *Emitter {
	if _, ok := e._rooms[room]; ok == false {
		e._rooms[room] = true
	}
	return e
}

// To ... Limit emission to a certain `room`.
func (e *Emitter) To(room string) *Emitter {
	return e.In(room)
}

// Of ... To Limit emission to certain `namespace`.
func (e *Emitter) Of(nsp string) *Emitter {
	e._flags["nsp"] = nsp
	return e
}

// Emit ... Send the packet.
func (e *Emitter) Emit(args ...interface{}) bool {
	rooms := []string{}

	if ok := len(e._rooms); ok > 0 {
		rooms = getKeys(e._rooms)
	}

	e._rooms = make(map[string]bool)

	return e.EmitTo(rooms, args...)
}

func (e *Emitter) EmitTo(rooms []string, args ...interface{}) bool {
	packet := make(map[string]interface{})
	extras := make(map[string]interface{})

	if ok := e.hasBin(args); ok {
		packet["type"] = binaryEvent
	} else {
		packet["type"] = event
	}

	packet["data"] = args

	if value, ok := e._flags["nsp"]; ok {
		packet["nsp"] = value
		delete(e._flags, "nsp")
	} else {
		packet["nsp"] = "/"
	}

	extras["rooms"] = rooms

	if ok := len(e._flags); ok > 0 {
		extras["flags"] = e._flags
	} else {
		extras["flags"] = make(map[string]string)
	}

	e._flags = make(map[string]string)

	//Pack & Publish
	chn := e._key + "#" + packet["nsp"].(string) + "#"

	b, err := msgpack.Marshal([]interface{}{uid, packet, extras})
	if err != nil {
		panic(err)
	} else {
		if ok := len(extras["rooms"].([]string)); ok > 0 {
			for _, room := range extras["rooms"].([]string) {
				chnRoom := chn + room + "#"
				publish(e, chnRoom, b)
			}
		} else {
			publish(e, chn, b)
		}
	}

	return true
}

func (e *Emitter) hasBin(args ...interface{}) bool {
	//NOT implemented yet!
	return true
}

func newPool(opts Options) *redis.Pool {
	return &redis.Pool{
		MaxIdle:   redisPoolMaxIdle,
		MaxActive: redisPoolMaxActive,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", opts.Host)
			if err != nil {
				return nil, err
			}

			if opts.Password != "" {
				if _, err := c.Do("AUTH", opts.Password); err != nil {
					c.Close()
					return nil, err
				}
				return c, err
			}

			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func publish(e *Emitter, channel string, value interface{}) {
	c := e._pool.Get()
	defer c.Close()
	c.Do("PUBLISH", channel, value)
}

func getKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))

	for key := range m {
		keys = append(keys, key)
	}

	return keys
}

// Print emitter details
func Dump(e *Emitter) {
	fmt.Println("Emitter key:", e._key)
	fmt.Println("Emitter flags:", e._flags)
	fmt.Println("Emitter rooms:", e._rooms)
}
