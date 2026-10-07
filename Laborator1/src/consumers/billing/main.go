// Command billing is the second consumer: it "creates an invoice" for each
// order. It rejects orders with a non-positive amount; such a message fails
// on every attempt and ends in the dead-letter queue.
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
