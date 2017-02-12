package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/robarchibald/configReader"
	"github.com/zobo/mrproxy/protocol"
	"github.com/zobo/mrproxy/proxy"
	"github.com/zobo/mrproxy/stats"
)

var version string

var configFile = flag.String("c", "/etc/redis/mrproxy.conf", "Config file location for mrproxy")
var logFile = flag.String("l", "/var/log/mrproxy.log", "Log file for mrproxy")

type proxyConfig struct {
	redisServer         string
	redisPort           int
	redisPassword       string
	redisMaxIdle        int
	redisMaxConnections int
	redisTimeoutSeconds int
	bindIPAddress       string
	bindPort            int
}

func main() {
	flag.Parse()
	c := proxyConfig{}
	err := configReader.ReadFile(*configFile, &c)
	if err != nil {
		log.Fatalln(err)
	}

	redis := fmt.Sprintf("%s:%d", c.redisServer, c.redisPort)
	pool := newConnPool(redis, c.redisPassword, c.redisMaxIdle, c.redisMaxConnections, c.redisTimeoutSeconds)

	bind := fmt.Sprintf("%s:%d", c.bindIPAddress, c.bindPort)
	l, err := net.Listen("tcp", bind)
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("Proxying Memcache from %s to Redis at %s", redis, bind)
	for {
		c, err := l.Accept()
		if err != nil {
			log.Println(err)
		}
		go processMc(c, pool)
	}
}

func newConnPool(address string, password string, maxIdle, maxConnections, timeoutSeconds int) *redis.Pool {
	timeout := time.Duration(timeoutSeconds) * time.Second
	return &redis.Pool{
		MaxIdle:   maxIdle,
		MaxActive: maxConnections,
		Dial: func() (redis.Conn, error) {
			tc, err := dial("tcp", address)
			if err != nil {
				return nil, err
			}
			c := redis.NewConn(tc, timeout, timeout)
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}

func dial(network, addr string) (net.Conn, error) {
	tcpAddr, err := net.ResolveTCPAddr(network, addr)
	if err != nil {
		return nil, err
	}
	tc, err := net.DialTCP(network, nil, tcpAddr)
	if err != nil {
		return nil, err
	}
	if err := tc.SetKeepAlive(true); err != nil {
		return nil, err
	}
	if err := tc.SetKeepAlivePeriod(2 * time.Minute); err != nil {
		return nil, err
	}
	return tc, nil
}

func processMc(c net.Conn, pool *redis.Pool) {
	defer log.Printf("%v end processMc", c)
	defer c.Close()

	stats.Connect()
	defer stats.Disconnect()

	// process
	br := bufio.NewReader(c)
	bw := bufio.NewWriter(c)

	// take it per need
	conn := pool.Get()
	defer conn.Close()

	redisProxy := proxy.NewRedisProxy(conn)
	proxy := stats.NewStatsProxy(redisProxy)

	for {
		req, err := protocol.ReadRequest(br)
		if perr, ok := err.(protocol.ProtocolError); ok {
			log.Printf("%v ReadRequest protocol err: %v", c, err)
			bw.WriteString("CLIENT_ERROR " + perr.Error() + "\r\n")
			bw.Flush()
			continue
		} else if err != nil {
			log.Printf("%v ReadRequest err: %v", c, err)
			return
		}

		res := proxy.Process(req)
		if !req.Noreply {
			bw.WriteString(res.Protocol())
			bw.Flush()
		}
	}
}
