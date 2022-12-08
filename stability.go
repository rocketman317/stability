package main

import (
	"log"
	"stability/database"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
)

func runStabilityTest(_ *cobra.Command, _ []string) {
	threads := viper.GetInt(keyThreads)
	log.Printf("Running stability test with [%d] threads", threads)

	params := database.GetDBParamsFromEnv(viper.GetViper())

	session, err := database.NewGocqlConfig(&params).CreateSession()
	if err != nil {
		log.Fatalf("failed to create session: %v", err)
	}

	// run a simple query to make sure the database is up and running
	if err := database.Ping(session); err != nil {
		log.Fatalf("failed to run a simple query: %v", err)
	}

	group := errgroup.Group{}

	for i := 0; i < threads; i++ {
		f := i
		group.Go(func() error {
			ticker := time.NewTicker(5 * time.Second)
			reporter := time.NewTicker(time.Minute)

			for {
				select {
				case <-reporter.C:
					// only 0 thread prints reports
					if f == 0 {
						log.Printf("Still running...")
					}
				case <-ticker.C:
					if err := database.Ping(session); err != nil {
						log.Fatalf("failed to run a simple query: %v", err)
					}
				}
			}
		})
	}

	if err := group.Wait(); err != nil {
		log.Fatalf("group error: %v", err)
	}
}
