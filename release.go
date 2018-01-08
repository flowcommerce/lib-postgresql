package main

import (
	"github.com/flowcommerce/tools/executor"
)

func main() {
	executor := executor.Create("lib-postgres")

	// publish this branch. TODO: remove after merge to master
	executor = executor.Add("git clone git@github.com:flowcommerce/misc.git")
	executor = executor.Add("cp misc/publish_branch/publish_branch.sh .")
	executor = executor.Add("chmod +x publish_branch.sh")
	executor = executor.Add("./publish_branch.sh ")

	// TODO: uncomment after merge to master
	//executor = executor.Add("dev tag")
	//executor = executor.Add("sbt +publish")

	executor.Run()
}
