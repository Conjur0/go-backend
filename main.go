// entry point

package main

func main() {
	log("Hello World!")
	initConfig()
	sqlInit()
	tablesInit()
	initClient()
	kjobInit()
	kpageInit()
	etagInit()
	tokensInit()
	getJobs()

	select {}
}
