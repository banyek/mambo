package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/go-sql-driver/mysql"
	"github.com/koding/logging"
	"gopkg.in/ini.v1"
)

var logger = logging.NewLogger("Mambo")

/*
Configuration parameters, mysql & statsd
*/
type configuration struct {
	mysqlHost  string // MySQL host to connect, if empty local socket will be used
	mysqlUser  string // User to connect MySQL with
	mysqlPass  string // Password for connecting MySQL
	mysqlDb    string // Database to connect to
	mysqlPort  int    // Port to connect MySQL, if left blank, 3306 will be used as default
	statsdHost string // statsd server hostname
	statsdPort int    // statsd server port, if left blank, 8125 will be used as default
}

/*
Commands
*/
type command struct {
	key      string // key to send statsd server (eg. mysql.slave01.bfc.kinja-ops.com.replication lag)
	query    string // query to run against mysql server. The output must be an integer
	interval time.Duration
}

type metric struct {
	key   string
	value int64
}

func main() {
	configfile := flag.String("cfg", "mambo.cfg", "Main configuration file")
	flag.Parse()
	logger.Notice("Mambo collector started")
	logger.Notice("Loading configuration from %s", *configfile)
	config, commands, err := configure(*configfile)
	if err != nil {
		logger.Critical("%v", err)
		os.Exit(1)
	}

	results := make(chan metric)
	for _, cmd := range commands {
		go controller(cmd, config, results)
	}
	logger.Notice("Data collector running")
	for result := range results {
		statsdSender(config, result)
	}
}

func controller(cmd command, config *configuration, results chan<- metric) {
	logger.Notice("Query loaded: %s", cmd.query)
	ticker := time.NewTicker(cmd.interval)
	defer ticker.Stop()
	for range ticker.C {
		result, err := mysqlWorker(config, cmd)
		if err != nil {
			logger.Error("Query for metric %q failed: %v", cmd.key, err)
			continue
		}
		results <- result
	}
}

func statsdSender(config *configuration, result metric) {
	address := net.JoinHostPort(config.statsdHost, strconv.Itoa(config.statsdPort))
	client, err := statsd.NewClient(address, "")
	if err != nil {
		logger.Error("Connect to StatsD: %v", err)
		return
	}
	defer client.Close()

	if err := client.Inc(result.key, result.value, 1.0); err != nil {
		logger.Error("Send metric %q: %v", result.key, err)
	}
}

func mysqlWorker(config *configuration, cmd command) (metric, error) {
	connector, err := mysql.NewConnector(mysqlConfig(config))
	if err != nil {
		return metric{}, err
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	var result int64
	if err := db.QueryRow(cmd.query).Scan(&result); err != nil {
		return metric{}, err
	}
	return metric{key: cmd.key, value: result}, nil
}

func mysqlConfig(config *configuration) *mysql.Config {
	cfg := mysql.NewConfig()
	cfg.User = config.mysqlUser
	cfg.Passwd = config.mysqlPass
	cfg.DBName = config.mysqlDb
	if config.mysqlHost == "" {
		cfg.Net = "unix"
	} else {
		cfg.Net = "tcp"
		cfg.Addr = net.JoinHostPort(config.mysqlHost, strconv.Itoa(config.mysqlPort))
	}
	return cfg
}

func configure(cfgfile string) (*configuration, []command, error) {
	file, err := ini.Load(cfgfile)
	if err != nil {
		return nil, nil, err
	}

	settings, err := file.GetSection("config")
	if err != nil {
		return nil, nil, fmt.Errorf("missing [config] section in %s", cfgfile)
	}
	statsdPortKey := "statsd_port"
	if !settings.HasKey(statsdPortKey) {
		statsdPortKey = "stats_port"
	}
	cfg := &configuration{
		mysqlHost:  settings.Key("mysql_host").String(),
		mysqlUser:  settings.Key("mysql_user").String(),
		mysqlPass:  settings.Key("mysql_pass").String(),
		mysqlDb:    settings.Key("mysql_db").String(),
		statsdHost: settings.Key("statsd_host").String(),
	}
	cfg.mysqlPort, err = parsePort(settings.Key("mysql_port"), 3306)
	if err != nil {
		return nil, nil, err
	}
	cfg.statsdPort, err = parsePort(settings.Key(statsdPortKey), 8125)
	if err != nil {
		return nil, nil, err
	}

	var commands []command
	for _, section := range file.Sections() {
		name := section.Name()
		if name == ini.DEFAULT_SECTION || name == "config" {
			continue
		}
		freq, err := section.Key("freq").Int64()
		if err != nil || freq <= 0 || freq > math.MaxInt64/int64(time.Millisecond) {
			return nil, nil, fmt.Errorf("[%s] freq must be a positive interval in milliseconds within the supported duration range", name)
		}
		commands = append(commands, command{
			key:      section.Key("key").String(),
			query:    section.Key("query").String(),
			interval: time.Duration(freq) * time.Millisecond,
		})
	}
	return cfg, commands, nil
}

func parsePort(key *ini.Key, defaultPort int) (int, error) {
	if key.String() == "" || key.String() == "0" {
		return defaultPort, nil
	}
	port, err := key.Int()
	if err != nil || port < 1 || port > 65535 {
		return 0, fmt.Errorf("[config] %s must be a port between 1 and 65535", key.Name())
	}
	return port, nil
}
