// Command billing is the second consumer: it "creates an invoice" for each
// order. It rejects orders with a non-positive amount; this is a permanent
// error, so the message goes to the dead-letter queue without retries.
package main

import (
	"errors"

	"pad-lab1/src/consumers/common"
	"pad-lab1/src/integration-service/protocol"
)

func main() {
	common.Run(common.Spec{
		Name:   "billing",
		Action: "create_invoice",
		Validate: func(o protocol.OrderCreated) error {
			if o.Amount <= 0 {
				return errors.New("cannot invoice a non-positive amount")
			}
			return nil
		},
	})
}
