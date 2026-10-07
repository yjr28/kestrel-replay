//go:build js && wasm

package main

import (
	"syscall/js"

	"github.com/yjr28/kestrel-replay/internal/browserdemo"
)

func main() {
	js.Global().Set("runKestrelGo", js.FuncOf(func(this js.Value, args []js.Value) any {
		if len(args) == 0 {
			return browserdemo.JSON(`{}`)
		}
		return browserdemo.JSON(args[0].String())
	}))
	select {}
}
