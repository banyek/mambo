Mambo
=====

MySQL - StatsD collector written in golang.
You can define commands which will be run against yor mysql server and the results of this queries will be sent to your statsd server.

Install
-------

```sh
$ go get github.com/banyek/mambo
```

Example usage
-------------

```sh
$ bin/mambo --cfg=/EXAMPLES/mambo.cfg
```

Example config file
-------------------

```
[config]

#mysql_host = localhost
#mysql_port = 3306
mysql_user = root
mysql_pass = root123
mysql_db = information_schema
statsd_host = 127.0.0.1
statsd_port = 8125

[query1]

key = mysql.server.query1
query = SELECT 1
freq = 1000

[query2]

key = mysql.server.query2
query = SELECT 2
freq = 500

[query3]

key = mysql.server.query3
query = SELECT 3
freq = 3000
```


