package database

import (
	"log"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func InitDatabaseFlags(fset *pflag.FlagSet) {
	fset.String("database", "127.0.0.1", "ip for connecting to the cassandra cluster")
	fset.String("datacenter", "DEV01", "local dc used for stats and replication purposes")
	fset.String("database-datacenter", "", "datacenter where to route queries")

	fset.String("database-tls-ca", "", "CA file to use for TLS validation (PEM format)")
	fset.String("database-tls-key", "", "Private key file for TLS client authorization (PEM format)")
	fset.String("database-tls-cert", "", "Certificate file for TLS client authorization (PEM format)")
	fset.String("database-tls-server-name", "", "The server name of the expected TLS certificate.")
	fset.Bool("database-no-tls", false, "do not use tls")
	fset.Bool("database-no-check-certificate", false, "skip validation of database certs")

	fset.String("database-engine", "cassandra", "database flavour ('cassandra' or 'scylla')")

	fset.String("keyspace", "rdrive_test", "Cassandra keyspace to use")

	fset.Int("database-connections", 2, "NumConn parameter of gocql. Connections to open toward each host")
	fset.Int("database-connection-timeout", 10, "connection timeout for cassandra in seconds")
	fset.Int("database-query-timeout-ms", 1200, "query timeout for cassandra")

	fset.Int("database-port", 9042, "port to use when connecting to cassandra")
	fset.Int("database-page-size", 100, "Cassandra CQL PageSize")

	fset.String("database-username", "", "username for the db connection")
	fset.String("database-password", "", "password for the db connection")
	fset.String("database-host-selection", "dc", "host selection policy ('token' or 'dc')")
}

func InitConfigFlag(fSet *pflag.FlagSet) {
	fSet.String("config", "", "config file to use")
}

func ReadConfigFileTarget(vip *viper.Viper) {
	configFile := vip.GetString("config")
	if configFile == "" {
		log.Fatalf("No config file set, set one with `--config`, " +
			"or ensure the necessary configuration are provided via ENV " +
			"variables or command-line flags. See --help")
		return
	}

	vip.SetConfigFile(configFile)
	vip.SetConfigType("yaml")

	// read config file
	err := vip.ReadInConfig()
	if err != nil {
		log.Fatalf("Error reading config file: %s: %s", viper.ConfigFileUsed(), err.Error())
	}
	log.Printf("Using config file: %s", viper.ConfigFileUsed())
}
