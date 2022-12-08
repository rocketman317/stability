package database

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gocql/gocql"
)

func NewGocqlConfig(params *Params) *gocql.ClusterConfig {
	conf := gocql.NewCluster(params.Hosts...)
	// Follow gocql.NewCluster() for the defaults

	// Let cassandra use its timestamp to determine the execution order
	// This solves problems (at least with a local cassandra cluster of one)
	// related to the millisecond resolution of the timestamps being sent.
	conf.DefaultTimestamp = false

	// Connecting to cassandra
	conf.Port = params.Port

	// Default PageSize
	conf.PageSize = params.PageSize

	// Default to LocalQuorum
	conf.Consistency = gocql.LocalQuorum

	// Connection timeout, ie: how many seconds
	// we will wait on a response
	// while connecting to node.
	conf.ConnectTimeout = time.Duration(params.ConnTimeout) * time.Second

	if params.ReconnectIntervalSec > 0 {
		conf.ReconnectInterval = time.Duration(params.ReconnectIntervalSec) * time.Second
	} else {
		// Try to reconnect to DOWN nodes every 30s
		conf.ReconnectInterval = 30 * time.Second
	}

	// After a connection error, we try to
	// attempt a new connection each second 3 times,
	// before marking host as DOWN.
	// At startup gocql blocks until first connection to every host is opened.
	// It internally uses a pool of connections for each host, but
	// first only is waited synchronously.
	// All hosts are connected asynchronously.
	conf.ReconnectionPolicy = &gocql.ConstantReconnectionPolicy{
		MaxRetries: 3,
		Interval:   1 * time.Second,
	}

	// experimental
	conf.SocketKeepalive = 5 * time.Second

	switch params.HostSelectionPolicy {
	case "token":
		// Select hosts to which each query is sent in this way:
		// - first try (randomly) the replicas for the key being queried
		// - then try the other hosts in the same DC
		// - then try the replicas for the key being queried in a remote DC
		// - then try a random host
		conf.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
			gocql.DCAwareRoundRobinPolicy(params.Datacenter),
			gocql.ShuffleReplicas())
	default:
		conf.PoolConfig.HostSelectionPolicy = gocql.DCAwareRoundRobinPolicy(params.Datacenter)
	}

	conf.NumConns = params.NumConns

	// Individual queries handling
	conf.Timeout = time.Duration(params.QueryTimeoutMs) * time.Millisecond
	conf.Keyspace = params.Keyspace

	if params.UseTLS {
		tlsConfig := tls.Config{
			InsecureSkipVerify: !params.VerifyCerts, //nolint: gosec
			CipherSuites: []uint16{
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
				tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			},
			MinVersion:       tls.VersionTLS12,
			CurvePreferences: []tls.CurveID{tls.X25519},
			ServerName:       params.TLSServerName,
		}

		if params.TLSKeyFile != "" && params.TLSCertFile != "" {
			cert, err := tls.LoadX509KeyPair(params.TLSCertFile, params.TLSKeyFile)

			if err != nil {
				panicMsg := fmt.Sprintf("Failed to load x509 keypair %s and %s: %s", params.TLSCertFile, params.TLSKeyFile, err.Error())
				log.Fatal(panicMsg)
			}

			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		if params.VerifyCerts && params.CAFile != "" {
			caCert, err := os.ReadFile(params.CAFile)
			if err != nil {
				log.Fatal(err.Error())
			}

			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tlsConfig.RootCAs = caCertPool
		}

		conf.SslOpts = &gocql.SslOptions{
			Config: &tlsConfig,
			// this is the inverse of InsecureSkipVerify above
			// this is from the gocql package, not from the tls package
			EnableHostVerification: params.VerifyCerts,
		}
	}

	if params.Username != "" && params.Password != "" {
		conf.Authenticator = gocql.PasswordAuthenticator{
			Username: params.Username,
			Password: params.Password.GetPlain(),
		}
	}

	return conf
}
