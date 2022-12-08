package main

import (
	"fmt"
	"log"
	"os"
	"stability/database"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const keyThreads = "threads"

var RootCmd = &cobra.Command{
	Use:   "rocket-s3",
	Short: "Rocket-S3 server",
}

var stabilityCmd = &cobra.Command{
	Use:    "test",
	Short:  "Cassandra connection stability testing tool",
	PreRun: LoadConf,
	Run:    runStabilityTest,
	Args:   cobra.NoArgs,
}

func init() {
	fl := stabilityCmd.PersistentFlags()

	database.InitConfigFlag(RootCmd.PersistentFlags())
	database.InitDatabaseFlags(fl)

	fl.Int(keyThreads, 1, "Number of threads to use. Default: 1")

	if err := viper.BindPFlags(RootCmd.PersistentFlags()); err != nil {
		log.Fatalf("Error while binding flags: %v", err)
	}

	if err := viper.BindPFlags(fl); err != nil {
		log.Fatalf("Error while binding flags: %v", err)
	}

	RootCmd.AddCommand(stabilityCmd)

}

func LoadConf(cmd *cobra.Command, _ []string) {
	database.ReadConfigFileTarget(viper.GetViper())
}

func main() {
	if err := RootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %+v\n", err)
		os.Exit(1)
	}
}
