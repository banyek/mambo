package main

import (
	"database/sql"                              // MySQL Query
	"flag"                                      // Command line parsing
	"fmt"                                       // Output formatting
	"github.com/cactus/go-statsd-client/statsd" // Statsd client
	_ "github.com/go-sql-driver/mysql"          // MySQL connection
	"github.com/koding/logging"                 // logging
	"gopkg.in/ini.v1"                           // ini file parsing
	"os"                                        // to exit with exitcode
	"strconv"                                   // string conversion
	"strings"                                   // string manipulation
	"time"                                      // timestamp logging, ticker
)

var logger = logging.NewLogger("Mambo")

/*
Configuration parameters, mysql & statsd
*/
type configuration struct {
	mysql_host  string // MySQL host to connect, if empty local socket will be used
	mysql_user  string // User to connect MySQL with
	mysql_pass  string // Password for connecting MySQL
	mysql_db    string // Database to connect to
	mysql_port  int    // Port to connect MySQL, if left blank, 3306 will be used as default
	statsd_host string // statsd server hostname
	statsd_port int    // statsd server port, if left blank, 8125 will be used as default
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
 statsd_host and statsd_port parameters
 For example:
 statsd_host = graphstatsd_port = 8125 -> url:"graph:8125"
*/

func statsdUriBuilder(config *configuration) string {
	uri := fmt.Sprint(config.statsd_host, ":", config.statsd_port)
	return uri

}

/*
 Connects statsd server and sends the metric
*/

func statsdSender(config *configuration, msg string) {
	client, err := statsd.NewClient(statsdUriBuilder(config), "")
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
	logger.Info("Statsd data flushed: %s", msg)
	client.Inc(key, value, 1.0)
}

func mysqlWorker(config *configuration, cmd command, results chan string) {
	var result string
	connecturi := mysqlUriBuilder(config)
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
	logger.Info("Data recieved from MySQL server: %s", res)
	results <- res
}

func mysqlUriBuilder(config *configuration) string {
	uri := ""
	if config.mysql_host == "" { // if mysql_host is not defined, we'll connect through local socket
		uri = fmt.Sprint(config.mysql_user, ":", config.mysql_pass, "@", "/", config.mysql_db)
	} else { // if we use TCP we'll also need the port of mysql too
		uri = fmt.Sprint(config.mysql_user, ":", config.mysql_pass, "@", config.mysql_host, ":", config.mysql_port, "/", config.mysql_db)
	}
	return uri
}

func configure(cfgfile string) (*configuration, []command) {
	var mysql_portc, statsd_portc int
	var cfg configuration
	commands := make([]command, 0)
	config, err := ini.Load(cfgfile)
	if err != nil {
		logger.Critical(err.Error())
		os.Exit(1)
	}
	sections := config.Sections()
	for _, section := range sections {
		if section.Name() != "DEFAULT" { //skip unnamed section
			if section.Name() == "config" { //[config] holds the configuratuin
				mysql_hostc := section.Key("mysql_host").String()
				mysql_userc := section.Key("mysql_user").String()
				mysql_passc := section.Key("mysql_pass").String()
				mysql_dbc := section.Key("mysql_db").String()
				// if mysql_port is not defined, we'll assume that the default 3306 will be used
				mysql_portc, err = section.Key("mysql_port").Int()
				if mysql_portc == 0 {
					mysql_portc = 3306
				}
				statsd_hostc := section.Key("statsd_host").String()
				// if statsd_port is not defined, we'll assume that the default 8125 will be used
				statsd_portc, err = section.Key("statsd_port").Int()
				if statsd_portc == 0 {
					statsd_portc = 8125
				}
				cfg = configuration{
					mysql_host:  mysql_hostc,
					mysql_user:  mysql_userc,
					mysql_pass:  mysql_passc,
					mysql_port:  mysql_portc,
					mysql_db:    mysql_dbc,
					statsd_host: statsd_hostc,
					statsd_port: statsd_portc,
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
