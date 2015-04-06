package main

import "github.com/fredrikbackstrom/kuling/kuling/command"

func main() {
	// Execute the command via the cobra command structure.
	command.AppCmd.Execute()
}
