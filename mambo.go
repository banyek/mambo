package main

import (
	"database/sql" // MySQL Query
	"flag"         // Command line parsing
	"fmt"          // Output formatting
	"os"           // to exit with exitcode
	"strconv"      // string conversion
	"strings"      // string manipulation
	"time"         // timestamp logging, ticker

	"github.com/cactus/go-statsd-client/statsd" // Statsd client
	_ "github.com/go-sql-driver/mysql"          // MySQL connection
	"github.com/koding/logging"                 // logging
	"gopkg.in/ini.v1"                           // ini file parsing
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
	key   string // key to send statsd server (eg. mysql.slave01.bfc.kinja-ops.com.replication lag)
	query string // query to run against mysql server. The output must be an integer
	freq  int    // what frequency the query should be run in milliseconds
}

func main() {
	// command line parameter parsing
	configfile := flag.String("cfg", "mambo.cfg", "Main configuration file")
	flag.Parse()
	logger := logging.NewLogger("Mambo")
	logger.Notice("Mambo collector started")
	logger.Notice("Loading configuration from %s", *configfile)
	// The 'results' channel will recive the results of the mysqlWorker queries
	results := make(chan string)
	config, commands := configure(*configfile) // Loading configuration and commands from ini file
	for _, command := range commands {
		go controller(command, config, results) // every command will launch a command controller
	}
	logger.Notice("Data collector running")
	for {
		select {
		// every time a MySQL worker yield data to the 'results' channel we call a statsdSender and we send that data to statsdserver
		case msg := <-results:
			{
				statsdSender(config, msg)
			}
		}
	}
}

/*
  The controller reads the command frequency (rate) from the command, and sets up
  a ticker with that frequency. We wait for the tick, and when it happens, we call
  a mysqlWorker with the command
*/
func controller(cmd command, cnf *configuration, results chan string) {
	logger.Notice("Query loaded: %s", cmd.query)
	tick := time.NewTicker(time.Millisecond * time.Duration(cmd.freq)).C // I have to convert freq to time.Duration to use with ticker
	for {
		select {
		case <-tick:
			mysqlWorker(cnf, cmd, results)
		}
	}
}

/*
  Builds up the statsd connect uri from
  statsdHost and statsdPort parameters
  For example:
  statsdHost = graphstatsdPort = 8125 -> url:"graph:8125"
*/
func statsdURIBuilder(config *configuration) string {
	uri := fmt.Sprint(config.statsdHost, ":", config.statsdPort)
	return uri

}

/*
  Connects statsd server and sends the metric
*/
func statsdSender(config *configuration, msg string) {
	client, err := statsd.NewClient(statsdURIBuilder(config), "")
	if err != nil {
		logger.Error(err.Error())
	}
	defer client.Close()
	arr := strings.Split(msg, ":")
	key := arr[0]
	value, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		logger.Error(err.Error())
	}
	//	logger.Info("Statsd data flushed: %s", msg)
	err = client.Inc(key, value, 1.0)
	if err != nil {
		logger.Error(err.Error())
	}
}

/*
  The mysqlWorker function connects to the database, runs the query which came from the command
  and puts the result to the results channel
*/
func mysqlWorker(config *configuration, cmd command, results chan string) {
	var result string
	connecturi := mysqlURIBuilder(config)
	db, err := sql.Open("mysql", connecturi)
	if err != nil {
		logger.Error(err.Error())
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		logger.Error(err.Error())
	}
	stmtOut, err := db.Prepare(cmd.query)
	if err != nil {
		logger.Error(err.Error())
	}
	defer stmtOut.Close()
	err = stmtOut.QueryRow().Scan(&result)
	if err != nil {
		logger.Error(err.Error())
	}
	res := fmt.Sprint(cmd.key, ":", result)
	//	logger.Info("Data recieved from MySQL server: %s", res)
	results <- res
}

/*
   Helper function to the mysqlWorker, it builds up the connect uri based on config
   if no mysqlHost is given, it tries to connect via local socket, and ignores the
   mysqlPort option.
*/
func mysqlURIBuilder(config *configuration) string {
	uri := ""
	if config.mysqlHost == "" { // if mysqlHost is not defined, we'll connect through local socket
		uri = fmt.Sprint(config.mysqlUser, ":", config.mysqlPass, "@", "/", config.mysqlDb)
	} else { // if we use TCP we'll also need the port of mysql too
		uri = fmt.Sprint(config.mysqlUser, ":", config.mysqlPass, "@", config.mysqlHost, ":", config.mysqlPort, "/", config.mysqlDb)
	}
	return uri
}

/*
  Builds up the configuration and command structs from the config file.
  It searches the [config] section for setting up configurations, and it
  assumes, that every other section will hold commands.
*/
func configure(cfgfile string) (*configuration, []command) {
	var mysqlPortc, statsdPortc int
	var cfg configuration
	//commands := make([]command, 0)
	var commands []command
	config, err := ini.Load(cfgfile)
	if err != nil {
		logger.Critical(err.Error())
		os.Exit(1)
	}
	sections := config.Sections()
	for _, section := range sections {
		if section.Name() != "DEFAULT" { //skip unnamed section
			if section.Name() == "config" { //[config] holds the configuratuin
				mysqlHostc := section.Key("mysql_host").String()
				mysqlUserc := section.Key("mysql_user").String()
				mysqlPassc := section.Key("mysql_pass").String()
				mysqlDbc := section.Key("mysql_db").String()
				// if mysqlPort is not defined, we'll assume that the default 3306 will be used
				mysqlPortc, err = section.Key("mysql_port").Int()
				if mysqlPortc == 0 {
					mysqlPortc = 3306
				}
				statsdHostc := section.Key("statsd_host").String()
				// if statsdPort is not defined, we'll assume that the default 8125 will be used
				statsdPortc, err = section.Key("stats_port").Int()
				if statsdPortc == 0 {
					statsdPortc = 8125
				}
				cfg = configuration{
					mysqlHost:  mysqlHostc,
					mysqlUser:  mysqlUserc,
					mysqlPass:  mysqlPassc,
					mysqlPort:  mysqlPortc,
					mysqlDb:    mysqlDbc,
					statsdHost: statsdHostc,
					statsdPort: statsdPortc,
				}
			} else { // here start the command parsing
				var cmd command
				keyc := section.Key("key").String()
				queryc := section.Key("query").String()
				freqc, _ := section.Key("freq").Int()
				cmd = command{
					key:   keyc,
					query: queryc,
					freq:  freqc,
				}
				commands = append(commands, cmd)
			}
		}
	}
	return &cfg, commands
}
