package emitter

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	msgpack "gopkg.in/vmihailenco/msgpack.v1"
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

// Emitter ... Socket.io Emitter
type Emitter struct {
	opts  Options
	key   string
	flags map[string]string
	rooms map[string]bool
	pool  *redis.Pool
}

// New ... Creates new Emitter using options
func New(opts Options) *Emitter {
	e := Emitter{opts: opts}

	if opts.RedisPool != nil {
		e.pool = opts.RedisPool
	} else {
		if opts.Host == "" {
			panic("Missing redis `host`")
		}
		e.pool = newPool(opts)
	}

	e.key = "socket.io"
	if opts.Key != "" {
		e.key = fmt.Sprintf("%s", opts.Key)
	}

	e.rooms = make(map[string]bool)
	e.flags = make(map[string]string)

	return &e
}

// In ... Limit emission to a certain `room`.`
func (e *Emitter) In(room string) *Emitter {
	if _, ok := e.rooms[room]; !ok {
		e.rooms[room] = true
	}
	return e
}

// To ... Limit emission to a certain `room`.
func (e *Emitter) To(room string) *Emitter {
	return e.In(room)
}

// Of ... To Limit emission to certain `namespace`.
func (e *Emitter) Of(nsp string) *Emitter {
	e.flags["nsp"] = nsp
	return e
}

// Emit ... Send the packet.
func (e *Emitter) Emit(args ...interface{}) bool {
	rooms := []string{}
	if len(e.rooms) > 0 {
		rooms = getKeys(e.rooms)
	}
	e.rooms = make(map[string]bool)
	return e.EmitTo(rooms, args...)
}

func (e *Emitter) EmitTo(rooms []string, args ...interface{}) bool {
	packet := make(map[string]interface{})
	extras := make(map[string]interface{})

	packet["type"] = event
	if e.hasBin(args) {
		packet["type"] = binaryEvent
	}

	packet["data"] = args
	packet["nsp"] = "/"
	if nsp, ok := e.flags["nsp"]; ok {
		packet["nsp"] = nsp
		delete(e.flags, "nsp")
	}

	extras["rooms"] = rooms
	extras["flags"] = make(map[string]string)
	if len(e.flags) > 0 {
		extras["flags"] = e.flags
	}

	e.flags = make(map[string]string)

	// Pack & Publish
	b, err := msgpack.Marshal([]interface{}{uid, packet, extras})
	if err != nil {
		panic(err)
	}

	ch := e.key + "#" + packet["nsp"].(string) + "#"

	if len(extras["rooms"].([]string)) < 1 {
		publish(e, ch, b)
		return true
	}

	for _, room := range extras["rooms"].([]string) {
		chRoom := ch + room + "#"
		publish(e, chRoom, b)
	}
	return true
}

func (e *Emitter) hasBin(args ...interface{}) bool {
	// NOT implemented yet!
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
	c := e.pool.Get()
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

// Dump ... Print emitter details
func Dump(e *Emitter) {
	fmt.Println("Emitter key:", e.key)
	fmt.Println("Emitter flags:", e.flags)
	fmt.Println("Emitter rooms:", e.rooms)
}
