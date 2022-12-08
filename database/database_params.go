package database

import (
	"github.com/spf13/viper"
	"log"
	"strings"
)

type ConsistencyLevel int

const (
	OneConsistency ConsistencyLevel = iota + 1
	TwoConsistency
	OneQuorumConsistency
	OneLocalQuorumConsistency
	LocalQuorumConsistency
	QuorumConsistency
	OneTwoConsistency
	AllConsistency
)

func (c ConsistencyLevel) String() string {
	switch c {
	case OneConsistency:
		return "one"
	case TwoConsistency:
		return "two"
	case LocalQuorumConsistency:
		return "localquorum"
	case OneLocalQuorumConsistency:
		return "one-localquorum"
	case OneQuorumConsistency:
		return "one-quorum"
	case QuorumConsistency:
		return "quorum"
	case AllConsistency:
		return "all"
	default:
		return "invalidConsistencyLevel"
	}
}

type Password string

func (p Password) String() string {
	if p == "" {
		return ""
	}
	return "REDACTED"
}

func (p Password) GetPlain() string {
	return string(p)
}

type Params struct {
	Name                 string
	Datacenter           string
	Hosts                []string
	Keyspace             string
	UseTLS               bool
	VerifyCerts          bool
	NumConns             int
	ConnTimeout          int
	QueryTimeoutMs       int
	Port                 int
	PageSize             int
	Consistency          ConsistencyLevel
	Username             string
	Password             Password
	Engine               string
	HostSelectionPolicy  string
	CAFile               string // TLS
	TLSKeyFile           string // TLS client auth
	TLSCertFile          string // TLS client auth
	TLSServerName        string // TLS client auth
	AutoretryMaxAttempt  int    // injected globally to lib/db.GlobalAutoretryMaxAttempt
	AutoretryDelayMs     int    // injected globally to lib/db.GlobalAutoretryDelayMs
	ReconnectIntervalSec int    // injected to conf.ReconnectInterval
}

// GetDBParamsFromEnv returns a Params struct from the Viper instance
func GetDBParamsFromEnv(viper *viper.Viper) Params {
	// supporting the old way used by Ansible to pass a list of hosts
	hosts := viper.GetStringSlice("hosts")
	hl := len(hosts)
	if hl == 0 {
		databaseStr := viper.GetString("database")
		if len(databaseStr) > 0 {
			// old style config: 1.1.1.1,host.astradbcloud
			hosts = strings.Split(databaseStr, ",")
		} else {
			// new style config:
			/*
			  database:
			    - 1.1.1.1
			    - host.astradbcloud
			*/
			hosts = viper.GetStringSlice("database")
			for k, host := range hosts {
				hosts[k] = strings.TrimSpace(host)
			}
			databaseStr = strings.Join(hosts, ",")
		}
	} else if hl == 1 {
		hosts = strings.Split(hosts[0], ",")
	}

	viper.SetDefault("database-connections", 2)
	viper.SetDefault("database-connection-timeout", 10)
	viper.SetDefault("database-query-timeout-ms", 1200)
	viper.SetDefault("database-port", 9042)
	viper.SetDefault("database-page-size", 100)
	viper.SetDefault("database-engine", "cassandra")
	viper.SetDefault("database-host-selection", "dc")

	var dbParams Params
	dbParams.Name = "Cassandra"
	dbParams.Hosts = hosts
	dbParams.VerifyCerts = !viper.GetBool("database-no-check-certificate")
	dbParams.UseTLS = !viper.GetBool("database-no-tls")
	dbParams.CAFile = viper.GetString("database-tls-ca")
	dbParams.TLSKeyFile = viper.GetString("database-tls-key")
	dbParams.TLSCertFile = viper.GetString("database-tls-cert")
	dbParams.TLSServerName = viper.GetString("database-tls-server-name")
	dbParams.AutoretryDelayMs = viper.GetInt("autoretry-delay-ms")
	dbParams.AutoretryMaxAttempt = viper.GetInt("autoretry-max-attempt")
	dbParams.ReconnectIntervalSec = viper.GetInt("reconnect-interval-sec")

	dbParams.Keyspace = viper.GetString("keyspace")

	dbParams.Datacenter = viper.GetString("database-datacenter")
	if dbParams.Datacenter == "" {
		dbParams.Datacenter = viper.GetString("datacenter")
	}

	dbParams.NumConns = viper.GetInt("database-connections")
	dbParams.ConnTimeout = viper.GetInt("database-connection-timeout")
	dbParams.QueryTimeoutMs = viper.GetInt("database-query-timeout-ms")
	dbParams.Port = viper.GetInt("database-port")
	dbParams.PageSize = viper.GetInt("database-page-size")
	dbParams.Username = viper.GetString("database-username")
	dbParams.Password = Password(viper.GetString("database-password"))
	dbParams.Engine = viper.GetString("database-engine")
	dbParams.HostSelectionPolicy = viper.GetString("database-host-selection")

	switch dbParams.Engine {
	case "cassandra", "scylla":
	default:
		log.Fatalf("Use either 'cassandra' or 'scylla' for "+
			"database-engine, current: '%s'\n", dbParams.Engine)
	}

	return dbParams
}
