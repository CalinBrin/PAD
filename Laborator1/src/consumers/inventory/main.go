// Command inventory is the first consumer: it "reserves stock" for each order.
package main

import "pad-lab1/src/consumers/common"

func main() {
	common.Run(common.Spec{Name: "inventory", Action: "reserve_stock"})
}
