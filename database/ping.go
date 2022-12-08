package database

import "github.com/gocql/gocql"

// Ping function actually pings the database by sending a simple query to it
func Ping(s *gocql.Session) error {
	_, err := s.Query("SELECT * FROM system.local").Iter().SliceMap()
	return err
}
